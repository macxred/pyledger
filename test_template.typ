// Test Template


// MaCX ReD Letterhead
#let macx_blue = rgb(29, 62, 110)
#set text(font: "DejaVu Sans", size: 11pt, lang: "de")
#set page(
  paper: "a4",
  margin: (top:4cm, bottom: 2cm, left: 2cm, right: 2cm),
  header: align(center, image("macxred_logo.png", width: 5cm)),
  footer: align(center, text(macx_blue, size: 7pt)[
    macx red AG #h(1em) Bundesstrasse 9 #h(1em) 6300 Zug #h(1em) Switzerland #h(1em) T +41 41 711 61 75 #h(1em) info\@macxred.ch #h(1em) MWSt CHE-113.177.507
  ])
)


// Dynamic table block
#eval(sys.inputs.table)
