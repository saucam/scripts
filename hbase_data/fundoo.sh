while read p; do
  echo $p | /opt/hbase/bin/hbase shell
done <create_tables
