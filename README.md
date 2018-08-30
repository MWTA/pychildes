# pychildes

A Python interface to [childes-db](http://childes-db.stanford.edu/index.html)

### example use

```python
import childes as chi

df = chi.get_transcripts(corpus=['Brown', 'Providence'])
```