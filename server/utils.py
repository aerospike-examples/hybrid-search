import urllib
import urllib.parse
from markdownify import MarkdownConverter

class EmbedTask(object):
    DOCUMENT="search_document"
    QUERY="search_query"

class MarkdownConvert(MarkdownConverter):
    def convert_hn(self, n, el, text, convert_as_inline):
        style = self.options['heading_style'].lower()
        if style == "none":
            text = text.strip()
            return text + "\n\n "
        return super().convert_hn(n, el, text, convert_as_inline)

def code_callback(el):
    if el.has_attr('class'):
        if len(el['class']) > 1 and el['class'][0] == "prism-code":
            return el['class'][1].split("-")[1]
    return None
    
options = {
    "escape_asterisks": False, 
    "escape_underscores": False,
    "code_language_callback": code_callback,
    "heading_style": "none",
    "strip": ["hr", "header", "footer", "img"]
}

def md(html):
    return MarkdownConvert(**options).convert(html)

def get_category(url):
    path = urllib.parse.urlparse(url).path
    path_parts = path.split("/")[1:]
    path_translation = {
        "lp": "marketing",
        "resources": path_parts[1],
        "s": "support"
    }

    return path_translation.get(path_parts[0]) or path_parts[0]