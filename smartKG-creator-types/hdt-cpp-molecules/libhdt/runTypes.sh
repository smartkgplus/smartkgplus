# increase the limit of open files
#ulimit -n 5000000 --> does not work
# we used this: https://gist.github.com/somandubey/52bff8c7cc8639292629
# vi /etc/security/limits.conf
# Add following 4 lines in above file
#  *  soft nproc  65535
#  *  hard nproc  65535
#  *  soft nofile 65535
#  *  hard nofile 65535

# -S to group by subjects
# -c to cut massive predicates
# -S to group by subjects
# -C to filter in certain classes


# for dbpedia
./tools/getFamilies -S -s nt/part_dbpedia2015_ -c -C dbpedia_classes_filtered.txt -e statsPart_dbpedia2015.json /backup_nfs/datasets/dbpedia2015/dbpedia2015.hdt 2> errDbpedia_stats_01_subj.txt > Dbpedia_stats_01_subj.txt
