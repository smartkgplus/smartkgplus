# increase the limit of open files
#ulimit -n 5000000 --> does not work
# we used this: https://gist.github.com/somandubey/52bff8c7cc8639292629
# vi /etc/security/limits.conf
# Add following 4 lines in above file
#  *  soft nproc  65535
#  *  hard nproc  65535
#  *  soft nofile 65535
#  *  hard nofile 65535

./tools/getFamilies -s part_watdiv.10M_ -e statsPart_watdiv.json data/watdiv.10M.hdt
# verbose
#./tools/getFamilies -v -s part_watdiv.10M_ -e statsPart_watdiv.json data/watdiv.10M.hdt > output


# Filter some families
#agrep 'Location;friendOf;likes;age;gender;givenName' one_line_statsPart_watdiv.json > c3_part_oneline.json

#remove someting before
#sed -i 's/^.* match//' parstForWatdivC3_fromJSON


# for dbpedia
#./tools/getFamilies -S -s part_dbpedia2015_ -e statsPart_dbpedia2015.json /backup_nfs/datasets/dbpedia2015/dbpedia2015.hdt 2> errDbpedia_stats_01_subj.txt > Dbpedia_stats_01_subj.txt
