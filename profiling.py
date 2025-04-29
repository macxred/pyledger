from pyinstrument import Profiler
from pyledger import TextLedger


profiler = Profiler()
profiler.start()

engine = TextLedger("~/macx/accounts")
engine.account_balance(account=1020, period="2024")

profiler.stop()
print(profiler.output_text(unicode=True, color=True))
