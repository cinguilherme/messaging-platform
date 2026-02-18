FROM clojure:tools-deps

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

# Cache deps
COPY deps.edn /usr/src/app/deps.edn
WORKDIR /usr/src/app
RUN ["clojure", "-M", "-e", ""]

# Run compilers
COPY . /usr/src/app
RUN ["clojure", "-M:duct", "-mvk", ":duct/compiler"]

# Set default port
ENV PORT=3000
EXPOSE 3000/tcp

CMD ["clojure", "-M:duct", "-mvk", ":duct/daemon"]
