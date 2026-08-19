.PHONY: build test lint fmt typecheck checkall

build:
	swift build

test:
	swift test

lint:
	swiftlint --strict

fmt:
	swiftformat .

typecheck:
	swift build

checkall: fmt lint typecheck test
