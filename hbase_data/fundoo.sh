while read p; do
  echo $p | /opt/bin/hbase shell
done <create_tables
