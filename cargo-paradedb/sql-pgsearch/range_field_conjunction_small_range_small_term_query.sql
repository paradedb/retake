SELECT * FROM benchmark_eslogs WHERE benchmark_eslogs @@@ '(aws_cloudwatch.log_stream:indigodagger OR metrics_size:[10 TO 20])';
