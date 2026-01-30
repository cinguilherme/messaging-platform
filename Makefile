build-image: 
	docker build -t core-service:latest .

run-container:
	docker run --network d-core_default -p 3000:3000 -p 3001:3001 -p 3002:3002 core-service:latest

dev:
	duct --main

lint:
	clojure -M:lint --lint src

tests:
	clojure -M:test

integration-tests:
	INTEGRATION=1 clojure -M:test

format:
	cljfmt check

format-fix:
	cljfmt fix

clean:
	rm -rf target
	rm -rf .cpcache
	rm -rf .nrepl-port
	rm -rf .lein-*


###  LOAD CLI ###
BASE_URL ?= http://localhost:3000
API_KEY_1 ?= dev-key-1
API_KEY_2 ?= dev-key-2
TOKEN1_FILE ?= .token1
TOKEN2_FILE ?= .token2
#CONV_ID ?= 1048a502-47a9-4ddd-ae67-881d0657ffa1
CONV_ID_FILE ?= .conv-id
.PHONY: auth-users load-user-1-conversation load-user-2-conversation

auth-users:
	@TOKEN1=$$(clojure -M -m core-service.dev.load-cli login --base-url $(BASE_URL) --api-key $(API_KEY_1) --user 1); \
	TOKEN2=$$(clojure -M -m core-service.dev.load-cli login --base-url $(BASE_URL) --api-key $(API_KEY_2) --user 2); \
	if [ -z "$$TOKEN1" ]; then echo "Token1 was empty; login failed."; exit 1; fi; \
	if [ -z "$$TOKEN2" ]; then echo "Token2 was empty; login failed."; exit 1; fi; \
	printf '%s' "$$TOKEN1" > $(TOKEN1_FILE); \
	printf '%s' "$$TOKEN2" > $(TOKEN2_FILE); \
	echo "TOKEN1 saved to $(TOKEN1_FILE)"
	@echo "TOKEN2 saved to $(TOKEN2_FILE)"

load-user-1-conversation:
	@TOKEN1=$$(cat $(TOKEN1_FILE) 2>/dev/null); \
	if [ -z "$$TOKEN1" ]; then echo "Missing token. Run: make auth-1"; exit 1; fi; \
	CONV_ID=$$(cat $(CONV_ID_FILE) 2>/dev/null); \
	if [ -z "$$CONV_ID" ]; then echo "Missing CONV_ID. Run: make create-conversation-test-users"; exit 1; fi; \
	clojure -M -m core-service.dev.load-cli load --base-url $(BASE_URL) --api-key $(API_KEY_1) --token "$$TOKEN1" --conversation-id "$$CONV_ID" --rate 65 --duration-s 240 --variable true --min-len 120 --max-len 220 --emoji true --emoji-rate 0.35

load-user-2-conversation:
	@TOKEN2=$$(cat $(TOKEN2_FILE) 2>/dev/null); \
	if [ -z "$$TOKEN2" ]; then echo "Missing token. Run: make auth-2"; exit 1; fi; \
	CONV_ID=$$(cat $(CONV_ID_FILE) 2>/dev/null); \
	if [ -z "$$CONV_ID" ]; then echo "Missing CONV_ID. Run: make create-conversation-test-users"; exit 1; fi; \
	clojure -M -m core-service.dev.load-cli load --base-url $(BASE_URL) --api-key $(API_KEY_2) --token "$$TOKEN2" --conversation-id "$$CONV_ID" --rate 65 --duration-s 240 --variable true --min-len 120 --max-len 220 --emoji true --emoji-rate 0.35

#get new conversation id for test users
create-conversation-test-users:
	@TOKEN1=$$(cat $(TOKEN1_FILE) 2>/dev/null); \
	TOKEN2=$$(cat $(TOKEN2_FILE) 2>/dev/null); \
	if [ -z "$$TOKEN1" ]; then echo "Missing token. Run: make auth-users"; exit 1; fi; \
	if [ -z "$$TOKEN2" ]; then echo "Missing token. Run: make auth-users"; exit 1; fi; \
	MEMBER_IDS=$$(printf '%s\n' "$$TOKEN1" "$$TOKEN2" | python3 -c 'import base64,json,sys; \
decode=lambda t: json.loads(base64.urlsafe_b64decode(t.split(".")[1] + "="*((4-len(t.split(".")[1])%4)%4))).get("sub",""); \
tokens=[l.strip() for l in sys.stdin.read().splitlines() if l.strip()]; \
subs=[decode(t) for t in tokens if t.count(".")>=1]; \
print(",".join([s for s in subs if s]))'); \
	if [ -z "$$MEMBER_IDS" ]; then echo "Missing member ids from tokens."; exit 1; fi; \
	CONV_JSON=$$(clojure -M -m core-service.dev.load-cli create-conversation --base-url $(BASE_URL) --api-key $(API_KEY_1) --token "$$TOKEN1" --member-ids "$$MEMBER_IDS"); \
	CONV_ID=$$(printf '%s' "$$CONV_JSON" | python3 -c 'import json,sys; data=json.load(sys.stdin); print((data.get("conversation") or {}).get("id",""))'); \
	if [ -z "$$CONV_ID" ]; then echo "Missing CONV_ID from response."; exit 1; fi; \
	printf '%s' "$$CONV_ID" > $(CONV_ID_FILE); \
	echo "CONV_ID saved to $(CONV_ID_FILE)"
