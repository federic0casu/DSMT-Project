##### inside VM1
- copy from local to remote the war file
- ./zookeeper-server-start.sh
- ./kakfa-server-start.sh
- move the war inside tomcat and start tomcat

##### inside VM2
- copy from local to remote the jar of flink
- ./kakfa-server-start.sh
