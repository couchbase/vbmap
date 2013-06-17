param nodes, integer, >= 1;
param slaves, integer, >= 0;

param tags_count, integer, >= 1;
param tags{0..nodes-1}, integer, >= 0, < tags_count;

var RI{i in 0..nodes-1, j in 0..nodes-1}, binary;

subject to zeros{i in 0..nodes-1, j in 0..nodes-1: tags[i] == tags[j]}: RI[i,j] = 0;
subject to active_balance{i in 0..nodes-1}: sum{j in 0..nodes-1} RI[i,j] = slaves;
subject to replica_balance{j in 0..nodes-1}: sum{i in 0..nodes-1} RI[i,j] = slaves;

var tag_max;
subject to tags_balance{i in 0..nodes-1, t in 0..tags_count-1}:
sum{j in 0..nodes-1} RI[i,j] * (if tags[j] == t then 1 else 0) <= tag_max;

minimize obj: tag_max;

solve;

for {i in 0..nodes-1}
{
  for {j in 0..nodes-1}
  {
    printf "%d\n", RI[i,j];
  }
}

end;
