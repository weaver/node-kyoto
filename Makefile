# kyoto -- Kyoto Cabinet bindings for Node.JS

## By default, just build
all:
	node-waf configure build

## For whatever reason, `npm publish` stopped honoring the
## `.npmignore` file a while back (or maybe I'm just doing it
## wrong). Do this as a workaround.
publish:
	(cd .. && tar cvzf /tmp/kyoto.tar.gz -X kyoto/.npmignore kyoto)
	npm publish /tmp/kyoto.tar.gz

## Run all unit tests.
test:
	expresso -s tests/*.js

clean:
	node-waf clean