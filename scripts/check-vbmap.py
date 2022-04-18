#!/usr/bin/env python3

"""
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

import json
import subprocess
import argparse
import math
from typing import Dict, List, Any, Optional, Callable

TagId = int
NodeId = int
TagSize = int


def format_tags(node_tag_map: Dict[NodeId, TagId]):
    result = ''
    for n in node_tag_map.keys():
        if result:
            result += ','
        result += '{}:{}'.format(n, node_tag_map[n])
    return result


class VbmapException(Exception):
    def __init__(self,
                 message: str,
                 node_tag_map: Dict[NodeId, TagId],
                 num_replicas: int,
                 description: str = ''):
        self.node_tag_map = node_tag_map
        self.numReplicas = num_replicas
        self.description = description
        super().__init__(message)

    def server_groups(self):
        return sorted({t: None for t in self.node_tag_map.values()})

    def get_node_tag_list(self) -> List[int]:
        return [self.node_tag_map[x] for x in sorted(self.node_tag_map)]

    def num_nodes(self) -> int:
        return len({x: None for x in self.node_tag_map})

    def __str__(self):
        return f'{super().__str__()}: ' \
               f'groups:{len(self.server_groups())} ' \
               f'nodes:{self.num_nodes()} ' \
               f'reps:{self.numReplicas} ' \
               f'node-tags:{self.get_node_tag_list()}, ' \
               f'{self.description}'


def run_vbmap(vbmap_path: str, node_tag_map: Dict[NodeId, TagId],
              num_replicas: int, num_vbuckets: int, greedy: bool) -> Any:
    command = [vbmap_path,
               '--num-nodes', str(len(node_tag_map)),
               '--num-replicas', str(num_replicas),
               '--num-vbuckets', str(num_vbuckets),
               '--output-format', 'json',
               '--relax-all']
    if len({t for t in node_tag_map.values()}) > 1:
        command += ['--tags', format_tags(node_tag_map)]
    if greedy:
        command += ['--greedy']
    result = subprocess.run(command, capture_output=True)
    if result.returncode:
        raise VbmapException(f'no flow found',
                             node_tag_map,
                             num_replicas,
                             f': {vbmap_path} returned exit code {result.returncode}')
    else:
        return json.loads(result.stdout)


def create_node_tag_map(server_group_sizes: List[int]) -> Dict[NodeId, TagId]:
    result = {}
    server_group_id = 0
    node_id = 0
    for size in server_group_sizes:
        for n in range(size):
            result[node_id] = server_group_id
            node_id += 1
        server_group_id += 1
    return result


def create_balanced_node_tag_map(server_group_count, server_group_size) -> Dict[NodeId, TagId]:
    return create_node_tag_map([server_group_size for _ in range(server_group_count)])


def make_tag_size_map(node_tag_map: Dict[NodeId, TagId]) -> Dict[TagId, TagSize]:
    result: Dict[TagId, TagSize] = {}
    tag: TagId
    for tag in node_tag_map.values():
        increment(result, tag, 1)
    return result


def get_server_group_size_permutations(
        server_group_count: int,
        min_server_group_size: int,
        max_server_group_size: int,
        suppress_duplicates: bool = False) -> List[List[int]]:
    if server_group_count == 1:
        return [[x] for x in range(min_server_group_size, max_server_group_size + 1)]
    partial: List[List[int]] = get_server_group_size_permutations(server_group_count - 1,
                                                                  min_server_group_size,
                                                                  max_server_group_size,
                                                                  suppress_duplicates)
    result = []
    already = {}
    for i in range(min_server_group_size, max_server_group_size + 1):
        for p in partial:
            to_append = p + [i]
            is_dupe = False
            if suppress_duplicates:
                key = tuple(sorted(to_append))
                if key in already:
                    is_dupe = True
                else:
                    already[key] = True
            if not is_dupe:
                result.append(to_append)
    return result


def fold(map: Dict[Any, int], key: Any, folder: Callable[[Any], Any]) -> None:
    map[key] = folder(map.get(key))


def increment(map: Dict[Any, int], key: Any, value: int) -> None:
    fold(map, key, lambda x: value + (x if x else 0))


class VbmapChecker:

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        pass


class RackZoneChecker(VbmapChecker):

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        tags = {t: None for t in node_tag_map.values()}
        if len(tags) <= 1:
            return
        for chain in chains:
            active_node = chain[0]
            active_tag = node_tag_map[active_node]
            replica_tags = {}
            for r in chain[1:]:
                replica_tag = node_tag_map[r]
                if replica_tag == active_tag:
                    raise VbmapException('not rack aware',
                                         node_tag_map,
                                         num_replicas)
                replica_tags[replica_tag] = True
            should_be = min(len(tags) - 1, num_replicas)
            actually_is = len(replica_tags)
            if actually_is < should_be:
                raise VbmapException('available server groups not maximally used',
                                     node_tag_map,
                                     num_replicas,
                                     f'chain: {chain} '
                                     f'used groups: {sorted(replica_tags.keys())} '
                                     f'avail groups: {sorted(set(tags) - {active_tag})}')


class ActiveBalanceChecker(VbmapChecker):

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        counts: Dict[int, int] = {}
        for chain in chains:
            increment(counts, chain[0], 1)
        max_active = max(counts, key=counts.get)  # type: ignore
        min_active = min(counts, key=counts.get)  # type: ignore
        if counts[max_active] - counts[min_active] > 5:
            raise VbmapException(f'not active balanced: '
                                 f'max: {max_active}, '
                                 f'min: {min_active} '
                                 f'counts {counts}',
                                 node_tag_map,
                                 num_replicas)


class ReplicaBalanceChecker(VbmapChecker):

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        counts: Dict[NodeId, int] = {}
        for chain in chains:
            for replica_node in chain[1:]:
                increment(counts, replica_node, 1)
        max_replicas: Dict[TagSize, int] = {}
        min_replicas: Dict[TagSize, int] = {}
        tag_sizes: Dict[TagId, TagSize] = make_tag_size_map(node_tag_map)
        node: NodeId
        size: TagSize
        for node in counts:
            size = tag_sizes[node_tag_map[node]]
            fold(max_replicas, size,
                 lambda x: max(counts[node], x) if x is not None else counts[node])
            fold(min_replicas, size,
                 lambda x: min(counts[node], x) if x is not None else counts[node])
        for size in max_replicas:
            max_count = max_replicas[size]
            min_count = min_replicas[size]
            if max_count - min_count > 5:
                groups: List[TagId] = [t for t in tag_sizes if tag_sizes[t] == size]
                max_node: NodeId = [n for n in counts if counts[n] == max_count][0]
                min_node: NodeId = [n for n in counts if counts[n] == min_count][0]
                raise VbmapException('not replica balanced',
                                     node_tag_map,
                                     num_replicas,
                                     f'group size: {size}, '
                                     f'groups: {sorted(groups)}, '
                                     f'max: {max_count}, '
                                     f'max_node: {max_node}, '
                                     f'min: {min_count}, '
                                     f'min_node: {min_node}, '
                                     f'counts: {[counts[x] for x in sorted(counts)]}')


class ActiveChecker(VbmapChecker):

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        nodes = {n: True for n in node_tag_map}
        if len(chains) != num_vbuckets:
            raise VbmapException(f'missing actives: # of actives: {len(chains)}',
                                 node_tag_map,
                                 num_replicas)
        vbucket = 0
        for chain in chains:
            if chain[0] not in nodes:
                raise VbmapException(f'active vbucket has invalid node',
                                     node_tag_map,
                                     num_replicas,
                                     f'vbucket: {vbucket}')
            vbucket += 1


class ReplicaChecker(VbmapChecker):

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        nodes = {n: True for n in node_tag_map}
        vbucket = 0
        replicas = 0
        chain: List[NodeId]
        for chain in chains:
            for replica_node in chain[1:]:
                if replica_node not in nodes:
                    raise VbmapException(f'replica vbucket has invalid node',
                                         node_tag_map,
                                         num_replicas,
                                         f'vbucket: {vbucket}, '
                                         f'chain: {chain}')
                replicas += 1
            vbucket += 1
        if replicas != num_vbuckets * num_replicas:
            raise VbmapException(f'fewer replicas than configured',
                                 node_tag_map,
                                 num_replicas,
                                 f'should be: {num_vbuckets * num_replicas}, are: {replicas}')


class RebalanceMoveChecker(VbmapChecker):

    def __init__(self, vbmap_path, num_vbuckets, greedy, verbose):
        self.vbmap_path = vbmap_path
        self.num_vbuckets = num_vbuckets
        self.greedy = greedy
        self.verbose = verbose

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        tags = {t for t in node_tag_map.values()}
        if len(tags) > 1:
            return
        next_node = max([n for n in node_tag_map]) + 1
        new_node_tag_map = dict(node_tag_map)
        new_node_tag_map[next_node] = next(iter(tags))
        new_chains = run_vbmap(self.vbmap_path,
                               new_node_tag_map,
                               num_replicas,
                               self.num_vbuckets,
                               self.greedy)
        idx = 0
        active_moves = 0
        new_replicas = 0
        for idx, chain in enumerate(chains):
            new_chain = new_chains[idx]
            new_active = chain[0] != new_chain[0]
            new_replica_vbuckets = set(new_chain[1:]) - set(chain)
            active_moves += 1 if new_active else 0
            new_replicas += 1 if len(new_replica_vbuckets) > 0 else 0
            if self.verbose and (new_active or len(new_replica_vbuckets) > 0):
                print(f'vbucket: {idx}, chain: {chain}, new_chain: {new_chain}')
        best_case = (num_replicas + 1) * math.ceil(num_vbuckets / len(node_tag_map))
        if active_moves + new_replicas > int(1.5 * best_case):
            raise VbmapException('too many new replicas built',
                                 node_tag_map,
                                 num_replicas,
                                 f'active moves: {active_moves}, '
                                 f'new_replicas: {new_replicas}, '
                                 f'total: {active_moves + new_replicas}, '
                                 f'best_case: {best_case}')


def print_checker_result(
        server_groups: List[int],
        num_replicas: int,
        vbmap_exception: Optional[VbmapException],
        checker: Optional[VbmapChecker],
        verbose: bool):
    if verbose:
        print('groups:{}, replicas: {} - {} {}{}'.format(
            server_groups,
            num_replicas,
            'not ok' if vbmap_exception else 'ok',
            vbmap_exception if vbmap_exception else '',
            type(checker).__name__ if checker else ''))
    else:
        print('x' if vbmap_exception else '.', end='', flush=True)


def check(vbmap_path: str,
          server_group_count: int,
          min_server_group_size: int,
          max_server_group_size: int,
          min_replicas: int,
          max_replicas: int,
          vbmap_num_vbuckets: int,
          checkers: List[VbmapChecker],
          verbose: bool = False,
          vbmap_greedy: bool = False):
    server_groups_list = get_server_group_size_permutations(server_group_count,
                                                            min_server_group_size,
                                                            max_server_group_size,
                                                            suppress_duplicates=True)
    exceptions = []
    for server_groups in server_groups_list:
        for num_replicas in range(min_replicas, max_replicas + 1):
            ve = None
            node_tag_map: Dict[int, int] = create_node_tag_map(server_groups)
            try:
                chains = run_vbmap(vbmap_path, node_tag_map, num_replicas,
                                   vbmap_num_vbuckets, vbmap_greedy)
                for checker in checkers:
                    vee = None
                    try:
                        checker.check(chains, node_tag_map, num_replicas,
                                      vbmap_num_vbuckets)
                    except VbmapException as e:
                        vee = e
                        exceptions.append(e)
                    print_checker_result(server_groups,
                                         num_replicas,
                                         vee,
                                         checker,
                                         verbose)
            except VbmapException as e:
                ve = e
                exceptions.append(ve)
            print_checker_result(server_groups, num_replicas, ve, None, verbose)
    if not verbose:
        print()
    return exceptions


def main(args):
    vbmap = args.vbmap_path
    if args.server_group_count < 1:
        print('server groups must be at least 1')
        exit(1)
    checkers = [ActiveChecker(),
                RackZoneChecker(),
                ActiveBalanceChecker(),
                ReplicaBalanceChecker(),
                ReplicaChecker()]
    if args.move_checker:
        checkers += [RebalanceMoveChecker(vbmap,
                                         args.vbmap_num_vbuckets,
                                         args.vbmap_greedy,
                                         args.verbose)]
    exceptions = check(vbmap,
                       args.server_group_count,
                       args.min_group_size,
                       args.max_group_size,
                       args.min_replicas,
                       args.max_replicas,
                       args.vbmap_num_vbuckets,
                       checkers,
                       verbose=args.verbose,
                       vbmap_greedy=args.vbmap_greedy)
    for ex in exceptions:
        print(ex)


DEFAULT_SERVER_GROUP_COUNT = 2
DEFAULT_MAX_GROUP_SIZE = 5
DEFAULT_MIN_GROUP_SIZE = 1
DEFAULT_MAX_REPLICAS = 3
DEFAULT_MIN_REPLICAS = 1
DEFAULT_VBMAP_NUM_VBUCKETS = 1024

parser = argparse.ArgumentParser(
    description='Runs vbmap to generate vbucket maps across a collection of sizes of '
                'server groups and replica counts and checks to see if the resulting '
                'map is balanced in terms of active and replica vbuckets and whether '
                'it honors rack-zone constraints.')
parser.add_argument('vbmap_path', help='path to vbmap executable')
parser.add_argument('--server-groups', dest='server_group_count', type=int,
                    default=DEFAULT_SERVER_GROUP_COUNT,
                    help='number of server groups (default {}).'.format(
                        DEFAULT_SERVER_GROUP_COUNT))
parser.add_argument('--max-group-size', dest='max_group_size', type=int,
                    default=DEFAULT_MAX_GROUP_SIZE,
                    help='max server group size (default {})'.format(
                        DEFAULT_MAX_GROUP_SIZE))
parser.add_argument('--min-group-size', dest='min_group_size', type=int,
                    default=DEFAULT_MIN_GROUP_SIZE,
                    help='min server group size (default {})'.format(
                        DEFAULT_MIN_GROUP_SIZE))
parser.add_argument('--max-replicas', dest='max_replicas', type=int,
                    default=DEFAULT_MAX_REPLICAS,
                    help='max number of replicas (default {})'.format(
                        DEFAULT_MAX_REPLICAS))
parser.add_argument('--min-replicas', dest='min_replicas', type=int,
                    default=DEFAULT_MIN_REPLICAS,
                    help='min number of replicas (default {})'.format(
                        DEFAULT_MIN_REPLICAS))
parser.add_argument('--move-checker', dest='move_checker', default=False, action='store_true',
                    help='run the move checker (only runs on single server groups)')
parser.add_argument('--verbose', dest='verbose', default=False, action='store_true',
                    help='emit verbose log information')
parser.add_argument('--vbmap-num-vbuckets', dest='vbmap_num_vbuckets', type=int,
                    default=DEFAULT_VBMAP_NUM_VBUCKETS,
                    help='number of vbuckets (default {}).'.format(
                        DEFAULT_VBMAP_NUM_VBUCKETS))
parser.add_argument('--vbmap-greedy', dest='vbmap_greedy', default=False,
                    action='store_true', help='generate the vbmap via the '
                    'greedy approach')

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)

