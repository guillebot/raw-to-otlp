docker build -t gschimmel/transformers-sevone-to-otlp:1.4 .
docker push gschimmel/transformers-sevone-to-otlp:1.4
docker tag gschimmel/transformers-sevone-to-otlp:1.4 gschimmel/transformers-sevone-to-otlp:latest
docker push gschimmel/transformers-sevone-to-otlp:latest

