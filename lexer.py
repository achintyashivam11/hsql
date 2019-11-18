import ply.lex as lex
# Reserved words
reserved = (
    'SELECT','LOAD','MAX','COUNT','SUM','USE','DATABASE','AS','INT','FLOAT','STR','FROM','WHERE','EXIT','CREATE','DROP','QUIT','TABLE','CURRENT','SCHEMA',"LIST"
)

tokens = reserved + (
    # Literals (identifier, integer constant, float constant, string constant,
    # char const)
    'ID', 'ICONST', 'FCONST', 'SCONST',

    # Operators (||, &&, !, <, <=, >, >=, ==, !=)
    'PLUS', 'MINUS', 'TIMES', 'DIVIDE', 'MOD',
    'OR', 'AND', 'NOT', 
    'LT', 'LE', 'GT', 'GE', 'EQ', 'NE',

    # Assignment (=)
    'EQUALS',
    # Delimeters ( ) , . ;
    'LPAREN', 'RPAREN',
    'COMMA', 'SEMI','COLON'
)

# Completely ignored characters
t_ignore = ' \t\x0c'

# Newlines


def t_NEWLINE(t):
    r'\n+'
    t.lexer.lineno += t.value.count("\n")

# Operators
t_OR = r'\|\|'
t_AND = r'&&'
t_NOT = r'!'
t_LT = r'<'
t_GT = r'>'
t_LE = r'<='
t_GE = r'>='
t_EQ = r'=='
t_NE = r'!='
t_COLON =r':'
# Assignment operators

t_EQUALS = r'='

# Delimeters
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_COMMA = r','
t_SEMI = r';'

# Identifiers and reserved words
typeid=[]
reserved_map = {}
for r in reserved:
    reserved_map[r.lower()] = r


def t_ID(t):
    r'[A-Za-z_]+(?:[.]+[A-Za-z]+)*'
    t.type = reserved_map.get(t.value, "ID")
    t.value=t.value
    return t

# Integer literal
t_ICONST = r'\d+([uU]|[lL]|[uU][lL]|[lL][uU])?'

# Floating literal
t_FCONST = r'((\d+)(\.\d+)(e(\+|-)?(\d+))? | (\d+)e(\+|-)?(\d+))([lL]|[fF])?'

# String literal
def t_SCONST(t):
    r"""('([^\\']+|\\'|\\\\)*')|("([^\\']+|\\'|\\\\)*")"""  # I think this is right ...
    t.value = t.value[1:-1] # .swapcase() # for fun
    return t

# Comments


def t_comment(t):
    r'(/\*(.|\n)*?\*/)|(//[^\n]*)'
    t.lexer.lineno += t.value.count('\n')

def t_error(t):
    print("Illegal character '%s' at line number %d and position %d " % (t.value[0],t.lineno,t.lexpos))
    t.lexer.skip(1)

lexer=lex.lex()

if __name__=='__main__':
    # Tokenize
    while True:
        lexer.input(input("hsql> "))
        tok = lexer.token()
        if not tok: 
            break      # No more input
        print(tok)
 