import pyparsing as pp

PREFIX = pp.Suppress('INFO  :')
COUNTER = pp.Word(pp.nums).setParseAction(lambda s, l, t: int(t[0]))
HYPHEN = pp.Literal('-').setParseAction(lambda s, l, t: 0)
COUNT_OR_UNDEF = (HYPHEN | COUNTER)

DONE = COUNT_OR_UNDEF
TOTAL = COUNT_OR_UNDEF

ACTIVE = pp.Suppress('+') + COUNTER
FAILED = pp.Suppress(',-') + COUNTER
ACTIVE_AND_FAILED = pp.Optional(pp.Suppress('(') + ACTIVE, default=0) + pp.Optional(FAILED, default=0) + pp.Optional(pp.Suppress(')'))

PROGRESS = pp.Group(DONE + ACTIVE_AND_FAILED + pp.Suppress('/') + TOTAL)

STAGE = (pp.Suppress('Map') | pp.Suppress('Reducer')) + pp.Word(pp.nums).suppress() + pp.Suppress(':') + PROGRESS

GRAMMAR = PREFIX + pp.OneOrMore(STAGE)

def parse(message):
    done, running, failed, total = (0, 0, 0, 0)

    try:
        stages = GRAMMAR.parseString(message)

        for stage in stages:
            done    += stage[0]
            running += stage[1]
            failed  += stage[2]
            total   += stage[3]

    except pp.ParseException:
        pass

    return (done, running, failed, total-done)
