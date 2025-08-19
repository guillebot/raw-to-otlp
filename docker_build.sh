docker build -t gschimmel/transformers-sevone-to-otlp:1.3 .
docker push gschimmel/transformers-sevone-to-otlp:1.3
docker tag gschimmel/transformers-sevone-to-otlp:1.3 gschimmel/transformers-sevone-to-otlp:latest
docker push gschimmel/transformers-sevone-to-otlp:latest

