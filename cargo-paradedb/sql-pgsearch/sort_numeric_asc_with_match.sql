SELECT * FROM benchmark_eslogs WHERE benchmark_eslogs @@@ 'log_file_path:"/var/log/messages/solarshark"' ORDER BY metrics_size ASC;
