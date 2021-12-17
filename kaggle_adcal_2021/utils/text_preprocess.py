# ref
# https://github.com/takapy0210/nlp_recipe/blob/af258c2d156d3d10071b0220936875d5053b8915/tokenize_ja/preprocessing.py

import re
import unicodedata

import neologdn

url_string = re.compile(r"(https?|ftp)(:\/\/[-_\.!~*\'()a-zA-Z0-9;\/?:\@&=\+$,%#]+)")

kaomoji_pattern = re.compile(
    r"\([^あ-ん\u30A1-\u30F4\u2E80-\u2FDF\u3005-\u3007\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF\U00020000-\U0002EBEF]+?\)"
)

emoji_pattern = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "]+",
    flags=re.UNICODE,
)


def clean_newline(text: str) -> str:
    """\nを除外する
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    text = re.sub(r"\n", "", text)
    return text


def clean_html(text: str) -> str:
    """urlを除外する
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    return url_string.sub(" ", text)


def clean_kaomoji(text: str) -> str:
    """顔文字を除外する
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    return kaomoji_pattern.sub(" ", text)


def clean_emoji(text: str) -> str:
    """絵文字を除外する
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    return emoji_pattern.sub("", text)


def clean_halfwidth_symbol(text: str) -> str:
    """半角記号を除外する
    対象の記号は下記の「ASCII印字可能文字」参照
    https://ja.wikipedia.org/wiki/ASCII
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    text = re.sub(r"[!-/:-@[-`{-~]", "", text)
    return text


def clean_fullwidth_symbol(text: str) -> str:
    """全角記号を除外する
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    text = re.sub(r"[︰-＠「」]", "", text)
    return text


def clean_hashtag(text: str) -> str:
    """ハッシュタグを除外する
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    text = re.sub(r"( #[a-zA-Z]+)+$", "", text)
    text = re.sub(r" #([a-zA-Z]+) ", r"\1", text)
    return text


def clean_reply(text: str) -> str:
    """リプライを削除
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    text = re.sub(r"(@\w+ )", "", text)
    return text


def normalize_unicodedata(text: str) -> str:
    """半角カタカナ、全角英数、ローマ数字・丸数字、異体字などなどの正規化
    例：㌔→キロ、①→1、ｷﾀｰ→キター、など
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    return unicodedata.normalize("NFKC", text)


def normalize_neologdn(text: str) -> str:
    """neologdnの正規化を行う
    日本語テキストに対してneologd辞書を用いる前に推奨される正規化（表記ゆれの是正）
    Args:
        text (str): 処理前のテキスト
    Returns:
        str: 処理後のテキスト
    """
    return neologdn.normalize(text)


def normalize_number(text: str, reduce: bool = True) -> str:
    """連続した数字を0で置換
    Args:
        text (str): 正規化する文字列
        reduce (bool): 数字の文字数を変化させるか否か. Defaults to False.
            例:
                Trueの場合「2万1870ドル」→「0万0ドル」
                Falseの場合「2万1870ドル」→「0万0000ドル」
    Returns:
        str: 正規化後の文字列
    """
    if reduce:
        return re.sub(r"\d+", "0", text)
    else:
        return re.sub(r"\d", "0", text)


def clean_text(text: str) -> str:
    """全ての前処理を実行するサンプル
    Args:
        text (str): 前処理前のテキスト
    Returns:
        str: 前処理後のテキスト
    """
    text = clean_newline(text)
    text = clean_html(text)
    text = clean_kaomoji(text)
    text = clean_emoji(text)
    text = clean_halfwidth_symbol(text)
    text = clean_fullwidth_symbol(text)
    text = clean_hashtag(text)
    text = clean_reply(text)
    text = normalize_unicodedata(text)
    text = normalize_neologdn(text)
    text = normalize_number(text)
    return text
