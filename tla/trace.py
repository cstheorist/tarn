"""Print a TLC counterexample compactly: step, action, and the variables that changed."""
import re, sys
txt = open(sys.argv[1]).read()
blocks = re.findall(r'State (\d+): <([^>]*)>\n(.*?)(?=\n\nState |\n\n\d+ states|\nError|\Z)', txt, re.S)
prev = {}
for n, act, body in blocks:
    name = act.split(' line')[0].strip()
    vs = dict(re.findall(r'/\\ (\w+) = (.*?)(?=\n/\\ |\Z)', body, re.S))
    vs = {k: re.sub(r'\s+', ' ', v) for k, v in vs.items()}
    changed = {k: v for k, v in vs.items() if prev.get(k) != v}
    prev = vs
    skip = {'everC', 'rtPred', 'started', 'dpos', 'nLoss', 'nFail', 'nCrash'}
    show = ' | '.join(f'{k}={v}' for k, v in changed.items() if k not in skip or n == '1')
    print(f'{n:>3} {name:28} {show[:400]}')
