# Documentation files for `spark_map`

Hey! This directory contains all the files used to compile the documentation for `spark_map`. The documentation files are written in Markdown, and, they are compiled (or "converted") to HTML files via [Quarto](https://quarto.org/) tool, with the objective to form a full website for the package.

Documentation Markdown files for `spark_map` are available in English and in Portuguese, and are stored in the `english` and `portugese` folders. The compiled HTML files from these Markdown files that composes the website are inside the `_site` folder.

To compile all the documentation files, you just need to open a terminal, and run the `quarto render` command inside this folder. But, in order to run this command, you need to have the [Quarto](https://quarto.org/) tool installed in your machine.

```
/pckg/spark_map/doc$ quarto render
```