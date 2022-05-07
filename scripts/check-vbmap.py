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
import statistics
from typing import Dict, List, Any, Optional, Callable
from enum import Enum

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


def run_vbmap(
        vbmap_path: str,
        node_tag_map: Dict[NodeId, TagId],
        num_replicas: int,
        num_vbuckets: int,
        num_slaves: int,
        greedy: bool,
        prev_vbmap_file: str,
        diag_file: str) -> Any:
    command = [vbmap_path,
               '--num-nodes', str(len(node_tag_map)),
               '--num-replicas', str(num_replicas),
               '--num-vbuckets', str(num_vbuckets),
               '--num-slaves', str(num_slaves),
               '--output-format', 'json',
               '--relax-all']
    if len({t for t in node_tag_map.values()}) > 1:
        command += ['--tags', format_tags(node_tag_map)]
    if greedy:
        command += ['--greedy']
    if prev_vbmap_file:
        command += ['--current-map', prev_vbmap_file]
    if diag_file:
        command += ['--diag', diag_file]

    # print(f'command: {command}')

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


def fold(map: Dict[Any, Any], key: Any, folder: Callable[[Any], Any]) -> None:
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


class PerNodeReplicaBalanceChecker(VbmapChecker):

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        counts: Dict[NodeId, Dict[NodeId, int]] = {}
        for chain in chains:
            active_node = chain[0]
            fold(counts, active_node, lambda x: {} if x is None else x)
            per_node_counts = counts[active_node]
            for replica_node in chain[1:]:
                increment(per_node_counts, replica_node, 1)
        max_replicas: Dict[NodeId, Dict[TagSize, int]] = {}
        min_replicas: Dict[NodeId, Dict[TagSize, int]] = {}
        tag_sizes: Dict[TagId, TagSize] = make_tag_size_map(node_tag_map)
        node: NodeId
        size: TagSize
        for active_node in counts:
            per_node_counts = counts[active_node]
            fold(max_replicas, active_node, lambda x: {} if x is None else x)
            fold(min_replicas, active_node, lambda x: {} if x is None else x)
            per_node_max_replicas = max_replicas[active_node]
            per_node_min_replicas = min_replicas[active_node]
            for replica_node in per_node_counts:
                size = tag_sizes[node_tag_map[replica_node]]
                fold(per_node_max_replicas, size,
                     lambda x: max(per_node_counts[replica_node], x)
                            if x is not None else per_node_counts[replica_node])
                fold(per_node_min_replicas, size,
                     lambda x: min(per_node_counts[replica_node], x)
                            if x is not None else per_node_counts[replica_node])
        for node in max_replicas:
            for size in max_replicas[node]:
                max_count = max_replicas[node][size]
                min_count = min_replicas[node][size]
                if max_count - min_count > 7:
                    groups: List[TagId] = [t for t in tag_sizes if tag_sizes[t] == size]
                    max_node: NodeId = [n for n in counts[node] if counts[node][n] == max_count][0]
                    min_node: NodeId = [n for n in counts[node] if counts[node][n] == min_count][0]
                    raise VbmapException(f'not replica balanced for active node {node}',
                                         node_tag_map,
                                         num_replicas,
                                         f'group size: {size}, '
                                         f'groups: {sorted(groups)}, '
                                         f'max: {max_count}, '
                                         f'max_node: {max_node}, '
                                         f'min: {min_count}, '
                                         f'min_node: {min_node}, '
                                         f'counts: {[counts[node][x] for x in sorted(counts[node])]}')


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

    @staticmethod
    def list_cmp(list1, list2):
        """ Compares two lists in the same as mb_map:listcmp/2.

        :param: list1 - first list
        :param: list2 - second list
        :return: if an element of list1 is less than a corresponding element of list2
        -1 is returned. If it's greater +1 is returned. Else the comparison
        moves to the next element. If one of the lists ends before a
        non-matching element is found, 0 is returned.
        """
        i1 = i2 = 0
        while True:
            if i1 < len(list1):
                if i2 < len(list2):
                    e1 = list1[i1]
                    e2 = list2[i2]
                    if e1 == e2:
                        i1 += 1
                        i2 += 1
                    elif e1 < e2:
                        return -1
                    else:
                        return 1
                else:
                    break
            else:
                break
        return 0

    @staticmethod
    def genmerge(cmp, list1, list2):
        """ Generic list merge following mb_map:genmerge/3.
        :param: cmp - the comparision function
        :param: list1 - first list
        :param: list2 - second list

        :return: 3-tuple where the first element is a list containing the items from the
        two lists that compare equal; the second element is a list containing the unused
        items from list1; the third element of the tuple is the unused items from list2
        """
        idx1 = idx2 = 0
        equal = []
        unused1 = []
        unused2 = []
        while True:
            if idx1 < len(list1):
                if idx2 < len(list2):
                    e1 = list1[idx1]
                    e2 = list2[idx2]
                    value = cmp(e1, e2)
                    if value == 0:
                        equal.append((e1, e2))
                        idx1 += 1
                        idx2 += 1
                    elif value < 0:
                        unused1.append(e1)
                        idx1 += 1
                    else:
                        unused2.append(e2)
                        idx2 += 1
                else:
                    unused1.extend(list1[idx1:])
                    break
            else:
                unused2.extend(list2[idx2:])
                break
        return equal, unused1, unused2

    @staticmethod
    def do_simple_minimize_moves(
            numbered_chains: List[tuple[int, List[NodeId]]],
            sorted_chains: List[List[NodeId]],
            shift: int):
        """ Part of simple move minimization following
        mb_map:do_simple_minimize_moves/3.
        :param numbered_chains: list of tuples of the form (vbucket-id, [chain])
        :param sorted_chains: list of chains
        :param shift:
        :return:
        """
        def ranker(map_entry):
            return map_entry[1][shift:]
        map2 = sorted(numbered_chains, key=ranker)

        def comparator(map_entry, chain):
            return RebalanceMoveChecker.list_cmp(map_entry[1][shift:], chain)
        return RebalanceMoveChecker.genmerge(comparator,
                                             map2,
                                             sorted_chains)

    @staticmethod
    def map_strip(
            chains: List[List[NodeId]],
            num_copies: int,
            keep_nodes: set[NodeId]) -> List[List[NodeId]]:
        result = []
        keep_set: set[NodeId] = {n for n in keep_nodes}
        for c in chains:
            result.append([(n if n in keep_set else -1) for n in c[:num_copies]])
        return result

    @staticmethod
    def map_chains(
            chains: List[List[NodeId]],
            node_map: Dict[NodeId, NodeId]) -> List[List[NodeId]]:
        result = []
        for c in chains:
            new_chain = [(x if x not in node_map else node_map[x]) for x in c]
            result.append(new_chain)
        return result

    @staticmethod
    def failover(
            chains: List[List[NodeId]],
            nodes: set[NodeId]) -> List[List[NodeId]]:
        result = []
        for c in chains:
            chain_len = len(c)
            new_chain = [x for x in c if x not in nodes]
            len_mismatch = chain_len - len(new_chain)
            if len_mismatch > 0:
                new_chain += ([-1] * len_mismatch)
            result.append(new_chain)
        return result

    @staticmethod
    def simple_minimize_moves(
            chains: List[List[NodeId]],
            new_chains: List[List[NodeId]],
            num_replicas: int,
            keep_nodes: set[NodeId],
            verbose: bool):
        """ Follows mb_map:simple_minimize_moves/4.

        :param chains: old replication chains
        :param new_chains: proposed new replication chains
        :param num_replicas: number of replicas
        :param keep_nodes: nodes that will remain after the rebalance
        :param verbose: whether or not to log verbosely
        :return: a alternative list of replication chains that attempt to
        minimize the vbucket moves
        """
        chains = RebalanceMoveChecker.map_strip(chains, num_replicas+1, keep_nodes)
        numbered_map = [(idx, chain) for idx, chain in enumerate(chains)]
        nm = numbered_map
        sc = sorted(new_chains)
        pairs = []
        for shift in range(0, num_replicas + 1 + 1):
            result = RebalanceMoveChecker.do_simple_minimize_moves(nm, sc, shift)
            pairs.extend(result[0])
            nm = result[1]
            sc = result[2]
        return [p[1] for p in sorted(pairs)]

    @staticmethod
    def check_minimized(minimized_chains, node_tag_map, num_replicas, num_vbuckets):
        checkers = [ActiveChecker(),
                    RackZoneChecker(),
                    ActiveBalanceChecker(),
                    ReplicaBalanceChecker(),
                    PerNodeReplicaBalanceChecker(),
                    ReplicaChecker()]
        exs = run_checkers(checkers, minimized_chains, node_tag_map, num_replicas,
                           num_vbuckets, False)
        if len(exs) > 0:
            raise VbmapException('check failure after rebalance: ',
                                 node_tag_map,
                                 num_replicas,
                                 ' '.join([ex.__str__() for ex in exs]))
            print('{ex}')
            exit(0)

    class Type(Enum):
        rebalance_in = 'in'
        rebalance_out = 'out'
        rebalance_swap = 'swap'
        after_failover = 'after_failover'
        after_dataloss = 'after_dataloss'
        add_replica = 'add_replica'
        remove_replica = 'remove_replica'

        def __str__(self):
            return self.value

    def __init__(self,
                 vbmap_path: str,
                 type_of_rebalance: str,
                 num_vbuckets: int,
                 num_slaves: int,
                 greedy: bool,
                 trials: int,
                 verbose: bool):
        self.rebalance_type = type_of_rebalance
        self.vbmap_path = vbmap_path
        self.num_vbuckets = num_vbuckets
        self.num_slaves = num_slaves
        self.greedy = greedy
        self.num_trials = trials
        self.verbose = verbose

    def compute_new_replicas_required(self, chains, new_chains):
        active_moves = 0
        new_replicas = 0
        total_moves = 0
        for idx, chain in enumerate(chains):
            new_chain = new_chains[idx]
            if chain != new_chain:
                total_moves += 1
            new_active = chain[0] != new_chain[0]
            active_moves += 1 if new_active else 0
            new_replica_vbuckets = len(set(new_chain) - set(chain))
            new_replicas += new_replica_vbuckets
            if self.verbose and chain != new_chain:
                print(f'vbucket: {idx}, '
                      f'chain: {chain}, '
                      f'new_chain: {new_chain} '
                      f'new replicas: {new_replica_vbuckets}')
        return active_moves, new_replicas, total_moves

    def do_check(self,
                 prev_vbmap_file: str,
                 chains: List[List[NodeId]],
                 node_tag_map: Dict[NodeId, TagId],
                 num_replicas: int,
                 num_vbuckets: int) -> tuple[list[list[int]], int, int, int]:
        new_chains = run_vbmap(self.vbmap_path,
                               node_tag_map,
                               num_replicas,
                               self.num_vbuckets,
                               self.num_slaves,
                               self.greedy,
                               prev_vbmap_file,
                               'diag.log')
        (u_active_moves, u_new_replicas, u_total_moves) = \
            self.compute_new_replicas_required(chains, new_chains)
        keep_nodes = {n for n in node_tag_map}
        if self.verbose:
            print(f'unmin: new replicas: {u_new_replicas}, '
                  f'active moves: {u_active_moves} '
                  f'total moves: {u_total_moves} ')
        minimized = RebalanceMoveChecker.simple_minimize_moves(chains,
                                                               new_chains,
                                                               num_replicas,
                                                               keep_nodes,
                                                               self.verbose)
        if len(minimized) < num_vbuckets:
            print(f'keep_nodes: {keep_nodes}')
            print(f'num-replicas: {num_replicas}')
            print(f'len minimized: {len(minimized)}')
            print(f'    chains = {chains}')
            print(f'    new-chains = {new_chains}')
            print(f'    minimized = {minimized}')
            raise VbmapException('some chains lost during simple move minimization',
                                 node_tag_map,
                                 num_replicas)
        (active_moves, new_replicas, total_moves) = \
            self.compute_new_replicas_required(chains, minimized)

        if self.verbose:
            print(f'min: new replicas: {new_replicas}, '
                  f'active moves: {active_moves} '
                  f'total moves: {total_moves} ')
        if u_new_replicas <= new_replicas:
            # use the original version
            active_moves = u_active_moves
            new_replicas = u_new_replicas
            minimized = new_chains
            total_moves = u_total_moves

        return minimized, active_moves, new_replicas, total_moves

    def check(self,
              chains: List[List[NodeId]],
              node_tag_map: Dict[NodeId, TagId],
              num_replicas: int,
              num_vbuckets: int) -> None:
        tags = {t for t in node_tag_map.values()}
        sorted_nodes = sorted([n for n in node_tag_map], reverse=True)
        max_node = max([n for n in node_tag_map])
        max_node_tag = node_tag_map[max_node]
        new_node_tag_map = dict(node_tag_map)
        if self.rebalance_type == RebalanceMoveChecker.Type.rebalance_in:
            # add in a single node
            new_node_tag_map[max_node + 1] = max_node_tag
        elif self.rebalance_type == RebalanceMoveChecker.Type.rebalance_out:
            # remove a single node (the max node)
            del new_node_tag_map[max_node]
        elif self.rebalance_type == RebalanceMoveChecker.Type.rebalance_swap:
            # map the vbuckets on max_node to max_node + 1, don't change the
            # node tag map
            chains = RebalanceMoveChecker.map_chains(chains, {max_node: max_node+1})
        elif self.rebalance_type == RebalanceMoveChecker.Type.after_failover:
            # failover the max node and remove it
            chains = RebalanceMoveChecker.failover(chains, {max_node})
            del new_node_tag_map[max_node]
        elif self.rebalance_type == RebalanceMoveChecker.Type.after_dataloss:
            # failover num_replicas + 1 nodes and remove them
            if len(node_tag_map) - num_replicas - 1 < 2:
                # skip this case
                return
            to_failover = sorted_nodes[:(num_replicas + 1)]
            for node in to_failover:
                del new_node_tag_map[node]
            chains = RebalanceMoveChecker.failover(chains, set(to_failover))
        elif self.rebalance_type == RebalanceMoveChecker.Type.add_replica:
            if num_replicas >= 3:
                # skip this case
                return
            num_replicas += 1
        elif self.rebalance_type == RebalanceMoveChecker.Type.remove_replica:
            if num_replicas == 0:
                # skip this case
                return
            num_replicas -= 1


        prev_vbmap_file = f'prev-vbmap.{num_replicas}.json'

        with open(prev_vbmap_file, "w") as f:
            json.dump(chains, f)

        (minimized, active_moves, new_replicas, total_moves) = \
            self.do_check(prev_vbmap_file,
                          chains,
                          new_node_tag_map,
                          num_replicas,
                          num_vbuckets)

        if self.num_trials > 1:
            all_new_replicas = [new_replicas]
            all_active_moves = [active_moves]
            all_total_moves = [total_moves]
            for i in range(self.num_trials):
                (c, active_moves, new_replicas, total_moves) = \
                    self.do_check(prev_vbmap_file,
                                  chains,
                                  new_node_tag_map,
                                  num_replicas,
                                  num_vbuckets)
                all_new_replicas += [new_replicas]
                all_active_moves += [active_moves]
                all_total_moves += [total_moves]
            print('total new replicas: avg: {}, stddev: {}, '
                  'active moves avg: {}, stddev: {}, '
                  'total moves avg: {}, stddev: {}, '.format(
                round(statistics.mean(all_new_replicas), 1),
                round(statistics.stdev(all_new_replicas), 1),
                round(statistics.mean(all_active_moves), 1),
                round(statistics.stdev(all_active_moves), 1),
                round(statistics.mean(all_total_moves), 1),
                round(statistics.stdev(all_total_moves), 1)))
        else:
            RebalanceMoveChecker.check_minimized(minimized,
                                                 new_node_tag_map,
                                                 num_replicas,
                                                 num_vbuckets)
            old_num_nodes = len(node_tag_map)
            new_num_nodes = len(new_node_tag_map)
            # at the end of the day, each node needs to carry at least 1/n of the vbuckets
            one_nth = math.floor(num_vbuckets * (num_replicas + 1) / new_num_nodes)
            diff_num_nodes = new_num_nodes - old_num_nodes
            diff_num_nodes = diff_num_nodes if diff_num_nodes >= 0 else -diff_num_nodes
            best_case = one_nth * diff_num_nodes
            if new_replicas > int(1.3 * best_case):
                raise VbmapException('too many new replicas built',
                                     node_tag_map,
                                     num_replicas,
                                     f'new_replicas: {new_replicas} '
                                     f'active moves: {active_moves} '
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


def run_checkers(
        checkers,
        chains,
        node_tag_map,
        num_replicas,
        num_vbuckets,
        verbose):
    tag_sizes = make_tag_size_map(node_tag_map)
    server_groups = [tag_sizes[k] for k in sorted(tag_sizes)]
    exceptions = []
    for checker in checkers:
        vee = None
        try:
            checker.check(chains, node_tag_map, num_replicas,
                          num_vbuckets)
        except VbmapException as e:
            vee = e
            exceptions.append(e)
        print_checker_result(server_groups,
                             num_replicas,
                             vee,
                             checker,
                             verbose)
    return exceptions

def check(vbmap_path: str,
          server_group_count: int,
          min_server_group_size: int,
          max_server_group_size: int,
          min_replicas: int,
          max_replicas: int,
          vbmap_num_vbuckets: int,
          vbmap_num_slaves: int,
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
                                   vbmap_num_vbuckets,
                                   vbmap_num_slaves,
                                   vbmap_greedy,
                                   '', '')
                exs = run_checkers(checkers, chains, node_tag_map, num_replicas,
                                   vbmap_num_vbuckets, verbose)
                exceptions.extend(exs)
            except VbmapException as e:
                ve = e
                exceptions.append(ve)
            print_checker_result(server_groups, num_replicas, ve, None, verbose)
    if not verbose:
        print()
    return exceptions

def run_simple_minimize_moves_tests():
    old_chains = [[0, 1], [1, 0]]
    new_chains = [[1, 0], [0, 1]]
    minimized = RebalanceMoveChecker.simple_minimize_moves(old_chains, new_chains,
                                                           1, {0, 1}, False)
    assert minimized == old_chains, 'new chains rearranged to look like old chains'
    old_chains = [[0, -1], [1, 0]]
    new_chains = [[1, 0], [0, 1]]
    minimized = RebalanceMoveChecker.simple_minimize_moves(old_chains, new_chains,
                                                           1, {0, 1}, False)
    assert minimized[1] == old_chains[1]
    assert minimized[0] == new_chains[1]

    old_chains = [[4, 2], [1, 0], [3, 0], [3, 2], [0, 3], [0, 1], [2, 4], [2, 1]]
    new_chains = [[4], [1], [3], [4], [0], [0], [2], [1]]
    minimized = RebalanceMoveChecker.simple_minimize_moves(old_chains, new_chains,
                                                           0, {0, 1, 2, 3, 4}, False)
    assert len(minimized) == len(old_chains)


def run_fold_test():
    mapping = {'a': 1, 'b': 2, 'c':3}
    fold(mapping, 'a', lambda x: x + 1)
    assert mapping['a'] == 2
    fold(mapping, 'd', lambda x: 1 if x is None else x + 1)
    assert mapping['d'] == 1


def run_rack_zone_tests():
    vbmap = [[11,9],[11,4],[11,1],[11,7],[11,0],[1,9],[1,8],[1,8],[1,7],[1,5],[0,9],[0,10],[0,6],
             [0,5],[0,7],[3,8],[3,10],[3,7],[3,6],[3,11],[10,3],[10,6],[10,8],[10,5],[10,0],[7,2],
             [7,4],[7,3],[7,11],[7,2],[7,10],[9,11],[9,1],[9,0],[9,4],[9,3],[9,2],[5,3],[5,2],[5,11],
             [5,1],[5,10],[4,5],[4,7],[4,11],[4,9],[4,8],[2,5],[2,6],[2,9],[2,8],[2,6],[2,5],[6,4],
             [6,4],[6,1],[6,0],[6,2],[6,10],[8,0],[8,4],[8,3],[8,1],[8,2]]

    node_tag_map = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0,
                    5: 1, 6: 1, 7: 1, 8: 1, 9: 1,
                    10: 2, 11: 2}
    checker = RackZoneChecker()
    try:
        checker.check(vbmap, node_tag_map, 1, 64)
    except VbmapException as e:
        print(f'excption in test: {e}')
        assert False



def run_tests():
    run_fold_test()
    run_simple_minimize_moves_tests()
    run_rack_zone_tests()


def main(args):
    vbmap = args.vbmap_path
    if args.server_group_count < 1:
        print('server groups must be at least 1')
        exit(1)
    if args.run_tests:
        run_tests()
        exit(0)
    checkers = [ActiveChecker(),
                RackZoneChecker(),
                ActiveBalanceChecker(),
                ReplicaBalanceChecker(),
                ReplicaChecker()]
    if args.move_checker:
        checkers += [RebalanceMoveChecker(vbmap,
                                          args.move_checker,
                                          args.vbmap_num_vbuckets,
                                          args.vbmap_num_slaves,
                                          args.vbmap_greedy,
                                          args.move_checker_trials,
                                          args.verbose)]
    exceptions = check(vbmap,
                       args.server_group_count,
                       args.min_group_size,
                       args.max_group_size,
                       args.min_replicas,
                       args.max_replicas,
                       args.vbmap_num_vbuckets,
                       args.vbmap_num_slaves,
                       checkers,
                       verbose=args.verbose,
                       vbmap_greedy=args.vbmap_greedy)
    for ex in exceptions:
        print(ex)


DEFAULT_SERVER_GROUP_COUNT = 2
DEFAULT_MAX_GROUP_SIZE = 5
DEFAULT_MIN_GROUP_SIZE = 1
DEFAULT_MAX_REPLICAS = 3
DEFAULT_MIN_REPLICAS = 0
DEFAULT_VBMAP_NUM_VBUCKETS = 1024
DEFAULT_VBMAP_NUM_SLAVES = 10

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
parser.add_argument('--move-checker', dest='move_checker',
                    type=RebalanceMoveChecker.Type, choices=list(RebalanceMoveChecker.Type),
                    help='run the move checker with the specified rebalance type')
parser.add_argument('--move-checker-trials', dest='move_checker_trials', type=int,
                    default=1,
                    help='run the move checker a specified number of times reporting average moves')
parser.add_argument('--verbose', dest='verbose', default=False, action='store_true',
                    help='emit verbose log information')
parser.add_argument('--vbmap-num-vbuckets', dest='vbmap_num_vbuckets', type=int,
                    default=DEFAULT_VBMAP_NUM_VBUCKETS,
                    help='number of vbuckets (default {}).'.format(
                        DEFAULT_VBMAP_NUM_VBUCKETS))
parser.add_argument('--vbmap-num-slaves', dest='vbmap_num_slaves', type=int,
                    default=DEFAULT_VBMAP_NUM_SLAVES,
                    help='number of slaves (default {}).'.format(
                        DEFAULT_VBMAP_NUM_SLAVES))
parser.add_argument('--vbmap-greedy', dest='vbmap_greedy', default=False,
                    action='store_true', help='generate the vbmap via the '
                    'greedy approach')
parser.add_argument('--run-tests', dest='run_tests', default=False,
                    action='store_true',
                    help='run internal tests')

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)

