
pdoc --html --output-dir server/blueprints/docs/ --force --template-dir server/blueprints/docs/  .

rm -rf server/blueprints/docs/static

mv server/blueprints/docs/project server/blueprints/docs/static

