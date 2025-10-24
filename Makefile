.PHONY: build hie

# Сгенерировать .cabal и hie.yaml
hie:
	hpack
	gen-hie > hie.yaml

# Собрать проект
build: hie
	cabal build
