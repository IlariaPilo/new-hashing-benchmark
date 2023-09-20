# Start from a basic Ubuntu image
FROM ubuntu:23.10

# Some arguments
ARG USER_ID
ARG GROUP_ID

# Install some stuff
RUN apt-get update && \
    apt-get install -y vim clang cmake sudo git
RUN apt-get install -y llvm libomp-dev
# to check for memory leaks
# RUN apt-get install -y valgrind

# # Create a new user called "aligner" (the default user)
# # Add it to the sudo group
# RUN groupadd -g ${GROUP_ID} benchmarker &&\
#     useradd -m -u ${USER_ID} -g benchmarker -G sudo benchmarker
# # Add password
# RUN echo "benchmarker:password" | chpasswd

# USER benchmarker

RUN echo 'echo "\
===================================\n\
=           benchmarker           =\n\
===================================\n\
"'\
> /root/.bashrc

WORKDIR /home/benchmarker/new-hashing-benchmark

# Run a shell as first operation
CMD ["/bin/bash"]