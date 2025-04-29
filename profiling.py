from pyinstrument import Profiler
from pyledger import TextLedger

engine = TextLedger("~/macx/accounts")
df = engine.journal.list()

profiler = Profiler()
profiler.start()

engine.sanitize_journal(df)

profiler.stop()
print(profiler.output_text(unicode=True, color=True))