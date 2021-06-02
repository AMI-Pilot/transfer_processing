.SUFFIXES: .dot .png .svg
.PHONY: all clean

all: dataflow.dot.png dataflow.dot.svg object_states.dot.svg object_states.dot.png

clean:
	rm *.png *.svg

%.dot.png: %.dot
	dot -O -T png $<

%.dot.svg: %.dot
	dot -O -T svg $<