import unidecode
import spacy
import local_constants as lc


def get_mentions(text: str = None) -> list:
    mentions = [i for i in text.split(' ') if '@' in i]

    return mentions


def get_hashtags(text: str = None) -> list:
    tags = [i for i in text.split(' ') if '#' in i]

    return tags


def basic_clean(text: str = None) -> str:
    resp = text.lower().strip()
    resp = unidecode.unidecode(resp)

    return resp


def remove_stop_words(text: str = None,
                      nlp: spacy.language = None,
                      extra_words: list = lc.extra_words) -> str:
    stop_w = nlp.Defaults.stop_words

    if extra_words is not None:
        for i in extra_words:
            stop_w.add(i)

    text = nlp(text)
    text = [str(word) for word in text if not str(word) in list(stop_w)]
    text = ' '.join(text)

    return text


def deep_clean(text: str = None,
               replace: dict = None,
               remove_mentions: bool = True,
               remove_hashtags: bool = False,
               remove_stop_w: bool = True) -> str:
    mentions = get_mentions(text)
    tags = get_hashtags(text)

    if replace is not None:
        for key in replace.keys():
            text = text.replace(key, replace[key])

    if remove_mentions:
        text = ' '.join([i for i in text.split(' ') if i not in mentions])

    if remove_hashtags:
        text = ' '.join([i for i in text.split(' ') if i not in tags])

    text = basic_clean(text)

    if remove_stop_w:
        text = remove_stop_words(text=text,
                                 nlp=spacy.load(lc.lenguage_model),
                                 extra_words=lc.extra_words
                                 )

    return text
