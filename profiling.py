from pyinstrument import Profiler
from pyledger import TextLedger


profiler = Profiler()
profiler.start()

engine = TextLedger("~/macx/rocket-accounting/accounts")
engine.account_balance(account=1000, period="2024")

profiler.stop()
print(profiler.output_text(unicode=True, color=True))
