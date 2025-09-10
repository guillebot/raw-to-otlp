VERSION=1.5
docker build -t gschimmel/transformers-raw-to-otlp:$VERSION .
docker push gschimmel/transformers-raw-to-otlp:$VERSION
docker tag gschimmel/transformers-raw-to-otlp:$VERSION gschimmel/transformers-raw-to-otlp:latest
docker push gschimmel/transformers-raw-to-otlp:latest

