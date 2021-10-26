sudo docker stop roneboq
sudo docker rm roneboq
sudo docker build -t boq-ubuntu20:latest -f docker/ubuntu20-ci/Dockerfile_boq .
sudo docker run -d --name roneboq --privileged --mount type=bind,source=/lib/modules,target=/lib/modules boq-ubuntu20
