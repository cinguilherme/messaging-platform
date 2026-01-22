build-image: 
	docker build -t core-service:latest .

run-container:
	docker run --network d-core_default -p 3000:3000 -p 3001:3001 -p 3002:3002 core-service:latest

dev:
	duct --main

lint:
	clj-kondo --lint src

test:
	clojure -M:test

format:
	cljfmt check

format-fix:
	cljfmt fix

clean:
	rm -rf target
	rm -rf .cpcache
	rm -rf .nrepl-port
	rm -rf .lein-*