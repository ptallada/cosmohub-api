import pyparsing as pp

PREFIX = pp.Suppress('INFO  :')
HYPHEN = pp.Literal('-').setParseAction(lambda s, l, t: '0')
COUNT = (HYPHEN | pp.Word(pp.nums)).setParseAction(lambda s, l, t: int(t[0]))
ACT_COUNT = pp.Suppress('(+') + COUNT + pp.Suppress(')')
PROGRESS = pp.Group(COUNT + pp.Optional(ACT_COUNT, default=0) + pp.Suppress('/') + COUNT)
STAGE = (pp.Suppress('Map') | pp.Suppress('Reducer')) + pp.Word(pp.nums).suppress() + pp.Suppress(':') + PROGRESS

GRAMMAR = PREFIX + pp.OneOrMore(STAGE)

def parse_progress(message):
    done, active, total = (0.0, 0.0, 0.0)
    
    try:
        stages = GRAMMAR.parseString(message)
        
        for stage in stages:
            done += stage[0]
            active += stage[1]
            total += stage[2]
        
        if total > 0:
            done = done*100/float(total)
            active = active*100/float(total)
    
    except pp.ParseException:
        pass
    
    return (done, active)