BUILDDIR = build
DISTDIR = dist
DISTNAME = venv
DIST = $(DISTDIR)/$(DISTNAME)
LOCDIR = /usr/lib/slurp
BASEDIR = $(dir $(LOCDIR))

.PHONY: all clean distclean install uninstall

all: $(DIST)

$(DIST):
	python setup.py bdist_venv2 --dist-name $(DISTNAME) --location-dir $(LOCDIR) --extras es,sentry,newrelic,watch

clean:
	rm -rf $(BUILDDIR)
	rm -rf $(DISTDIR)
	
distclean: clean

$(DESTDIR)$(BASEDIR):
	mkdir -p $(DESTDIR)$(BASEDIR)

install: $(DESTDIR)$(BASEDIR) $(DIST)
	cp -R $(DIST)$(LOCDIR) $(DESTDIR)$(BASEDIR)

uninstall:
	rm -rf $(DESTDIR)$(LOCDIR)
