# twitter-translate
This repo is to translate scraped german tweets to English.

**First step:** 
this code relies on [googletrans library](https://pypi.org/project/googletrans/) please install it to your working environemt first.


Unfortunately, google translate API limits the translation requests. This code uses two strategies as a workaround:
1. Reducing the total number of characters: by removing mentions and links.

2. Reducing the number of API requests:
   Tweets are not translated individually, instead the tweets are accumulated into a bigger batch of tweets separated by a separator of your choice.
   - It's Extremely important to define a proper separator because the separators will be translated as well. Later on, you need the separators intact to separate the translated tweets again. I'm currently using " §§§BLANK§§§ " and still some of the "§" signs get changed after translation.
   - Although google translate can take up to 15k character. translation usually fails for requests longer than 5k characters. Therefore I sat the limit to 4500 characters per request.

3. waiting on Failure:
   - there is an error tolerance of 5 failed batch translations, if 5 batches in a row fail to be translated, the code will pause and wait for you to activate a new VPN. it will continue when ou press Enter.
   - (planned) In addition, a list of untranslated tweets will be output.
