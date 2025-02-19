import multiprocessing

# Gunicorn configuration
bind = "0.0.0.0:8000"
workers = 2  # Réduit le nombre de workers pour économiser la mémoire
threads = 1  # Un seul thread par worker pour plus de stabilité
timeout = 120
worker_class = 'sync'
max_requests = 100  # Réduit pour éviter les fuites de mémoire
max_requests_jitter = 10
worker_tmp_dir = "/dev/shm"
forwarded_allow_ips = '*'
keepalive = 65

# Memory optimization
limit_request_line = 4096
limit_request_fields = 100
limit_request_field_size = 8190
graceful_timeout = 60
worker_abort_on_error = True

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'
capture_output = True
enable_stdio_inheritance = True

# Performance tuning
backlog = 2048
preload_app = False  # Désactivé pour réduire l'utilisation de la mémoire