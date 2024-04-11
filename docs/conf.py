# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Hashquery"
copyright = "2024, Hashboard"
author = "Hashboard"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "myst_parser",
    "sphinx_design",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "README.md"]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "pyarrow": ("https://arrow.apache.org/docs", None),
}

# -- Autodoc configuration -------------------------------------------------

autodoc_default_options = {
    "member-order": "bysource",
    "exclude-members": "__weakref__, to_wire_format, from_wire_format",
}
autodoc_typehints_format = "short"
autodoc_preserve_defaults = True
maximum_signature_line_length = 100
add_module_names = False
python_display_short_literal_types = True
napoleon_google_docstring = True

# -- Markdown configuration -------------------------------------------------

myst_enable_extensions = [
    "amsmath",
    "attrs_inline",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = ["css/overrides.css"]
html_js_files = ["js/jquery.js", "js/markup.js", "js/analytics.js"]
html_theme_options = {
    # options here:
    # https://sphinx-rtd-theme.readthedocs.io/en/latest/configuring.html
    # the defaults are fine; they don't give us much to work with
}
