import sys
import time
from gensim.corpora.textcorpus import *
# import multiprocessing
import os


def tokenize(content):     # override original method in wikicorpus.py in order to preserve punctuation
    # max len token was 15
    return [token.encode('utf8') for token in content.split()
            if len(token) <= 25 and not token.startswith('_')]


# def process_article(args):    # override original method in wikicorpus.py
#     text, lemmatize, title, pageid = args
#     text = filter_wiki(text)
#     if lemmatize:
#         result = utils.lemmatize(text)
#     else:
#         result = tokenize(text)
#     return result, title, pageid


def extract_posts(fn, max_post):
    # TODO: read from cleaned txt now. Implement reading from json objects later, including preprocess and metadata
    for idx, line in enumerate(open(fn, 'r')):
        if max_post and idx >= max_post:
            break
        line = line.strip()
        # print(line)
        if len(line):
            text = line
            yield text


class MyRedditCorpus(TextCorpus):
    def __init__(self, input=None, dictionary=None, metadata=False, character_filters=None, tokenizer=None,
                 token_filters=None, max_post=None):
        self.max_post = max_post
        TextCorpus.__init__(self, input, dictionary, metadata, character_filters, tokenizer, token_filters)

    def get_texts(self):
        num_posts, positions = 0, 0
        texts = [text for text in extract_posts(self.input, self.max_post)]

        for text in texts:
            if text == '[deleted]':
                # print('there is a deleted post')
                continue

            tokens = tokenize(text)
            num_posts += 1
            positions += len(tokens)

            yield tokens

        self.length = num_posts

        # pool = multiprocessing.Pool(self.processes)
        # for group in utils.chunkize(texts, chunksize=10 * self.processes, maxsize=1):
        #     for tokens, title, pageid in pool.imap(process_article, group):
        #         articles_all += 1
        #         positions_all += len(tokens)
        #         if len(tokens) < ARTICLE_MIN_WORDS or any(title.startswith(ignore + ':') for
        #                                                   ignore in IGNORED_NAMESPACES):
        #             continue
        #         articles += 1
        #         positions += len(tokens)
        #         if self.metadata:
        #             yield (tokens, (pageid, title))
        #         else:
        #             yield tokens
        # pool.terminate()
        # self.length = articles  # cache corpus length


def make_reddit_corpus(corpus, text_file_dir):
    reddit = MyRedditCorpus(input=corpus, max_post=None)
    print("finished initializing")
    i = 0
    texts = reddit.get_texts()
    for text in texts:
        # print(text)
        file_add = text_file_dir + str(i).zfill(8)
        file = open(file_add, 'w')
        file.write(b' '.join(text).decode('utf-8') + '\n')
        file.close()
        i = i + 1
        if i % 10000 == 0:
            print('Processed ' + str(i) + ' posts')
    print('Processing complete!')


if __name__ == '__main__':
    start = time.time()
    # input_file = "enwiki-latest-pages-articles.xml.bz2"
    input_file = "/export/c10/pxu/data/tobacco/clean_reddit_text.txt"
    # output_dir = "textFiles/"
    output_dir = "reddit_textFiles/"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    make_reddit_corpus(input_file, output_dir)

    print("Processing time:", time.time()-start)

