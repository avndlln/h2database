# run the benchmarks with db #9, defined in test.props, an in-memory columnar database
./build.sh compile && java -Djava.util.logging.config.file=/home/ubuntu/h2/logging.properties -cp temp/ org.h2.test.bench.TestPerformance -db 9 > bench.out
