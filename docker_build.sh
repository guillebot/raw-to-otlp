docker build -t gschimmel/transformers-raw-to-otlp:1.4 .
docker push gschimmel/transformers-raw-to-otlp:1.4
docker tag gschimmel/transformers-raw-to-otlp:1.4 gschimmel/transformers-raw-to-otlp:latest
docker push gschimmel/transformers-raw-to-otlp:latest

