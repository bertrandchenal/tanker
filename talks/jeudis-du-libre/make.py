import re

from pygments import highlight
from pygments.formatters import HtmlFormatter, ClassNotFound
from pygments.lexers import get_lexer_by_name
import misaka

class HighlighterRenderer(misaka.HtmlRenderer):
    def blockcode(self, text, lang):
        try:
            lexer = get_lexer_by_name(lang, stripall=True)
        except ClassNotFound:
            lexer = None

        if lexer:
            formatter = HtmlFormatter()
            return highlight(text, lexer, formatter)
        # default
        return '\n<pre><code>{}</code></pre>\n'.format(text.rstrip())

md = misaka.Markdown(
    HighlighterRenderer(),
    extensions=('fenced-code', 'tables'))
section_tpl = '<section class="slide" id="s{id}">{section}</section>'
hline_re = re.compile('\n---\s*\n')


def read_file(path):
    content = open(path).read()
    for chunk in hline_re.split(content):
        yield chunk

def to_html(chunk_id, chunk):
    section = md(chunk)
    return section_tpl.format(id=chunk_id, section=section)

def main():
    content = '\n'.join(
        to_html(cid, chunk)
        for cid, chunk in enumerate(read_file('tanker.md')))
    page_tpl = open('tanker-tpl.html').read()
    print(page_tpl % {'article': content, 'title': 'Tanker'})

if __name__ == '__main__':
    main()
