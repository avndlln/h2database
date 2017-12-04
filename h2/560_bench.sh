./build.sh compile && java -cp temp/ org.h2.test.bench.TestPerformance -db 1 &> bench_1.out 
cp benchmark.html benchmark_h2.html

# run the benchmarks with db #9, defined in test.props, an in-memory columnar database
./build.sh compile && java -cp temp/ org.h2.test.bench.TestPerformance -db 9 &> bench_9.out 
cp benchmark.html benchmark_columnar.html
