import praw
import pandas as pd

# Configuration de l'application Reddit
reddit = praw.Reddit(
    client_id="FusD-mrVZlHaRNgHsY-zFA",
    client_secret="0BuSu8SVt6wYg2jyLbM-17IKJ8WstA",
    user_agent="Palestine:Conflit Israélo-Palestinien Analysis v1.0 by /u/your_reddit_username"
)

# Fonction pour extraire les données en fonction des mots-clés
def fetch_reddit_data_by_keyword(subreddit_name, keywords, post_limit=10):
    """
    Extrait les posts et commentaires contenant des mots-clés spécifiques.
    :param subreddit_name: Nom du subreddit à explorer.
    :param keywords: Liste des mots-clés à rechercher.
    :param post_limit: Nombre de posts à analyser.
    :return: Un DataFrame contenant les informations.
    """
    subreddit = reddit.subreddit(subreddit_name)
    data = {
        "post_id": [],
        "post_title": [],
        "post_author": [],
        "post_score": [],
        "post_url": [],
        "post_num_comments": [],
        "comment_id": [],
        "comment_body": [],
        "comment_author": [],
        "comment_score": [],
    }

    for post in subreddit.search(" OR ".join(keywords), limit=post_limit):
        post_id = post.id
        post_title = post.title
        post_author = post.author.name if post.author else "N/A"
        post_score = post.score
        post_url = post.url
        post_num_comments = post.num_comments

        # Ajouter les informations du post
        data["post_id"].append(post_id)
        data["post_title"].append(post_title)
        data["post_author"].append(post_author)
        data["post_score"].append(post_score)
        data["post_url"].append(post_url)
        data["post_num_comments"].append(post_num_comments)
        data["comment_id"].append(None)
        data["comment_body"].append(None)
        data["comment_author"].append(None)
        data["comment_score"].append(None)

        # Extraction des commentaires
        post.comments.replace_more(limit=0)
        for comment in post.comments.list():
            data["post_id"].append(post_id)
            data["post_title"].append(post_title)
            data["post_author"].append(post_author)
            data["post_score"].append(post_score)
            data["post_url"].append(post_url)
            data["post_num_comments"].append(post_num_comments)
            data["comment_id"].append(comment.id)
            data["comment_body"].append(comment.body)
            data["comment_author"].append(comment.author.name if comment.author else "N/A")
            data["comment_score"].append(comment.score)

    # Convertir en DataFrame
    df = pd.DataFrame(data)
    return df

# Exemple d'utilisation
if __name__ == "__main__":
    subreddits = ["worldnews", "politics", "MiddleEastNews", "Israel", "Palestine"]
    keywords = ["Israel", "Palestine", "Gaza", "Hamas", "Fatah", "conflit", "paix"]
    post_limit = 20

    # Extraire les données pour chaque subreddit
    for subreddit_name in subreddits:
        df = fetch_reddit_data_by_keyword(subreddit_name, keywords, post_limit)

        # Afficher les premières lignes
        print(f"Subreddit: {subreddit_name}")
        print(df.head())

        # Sauvegarder dans un fichier CSV
        output_file = f"{subreddit_name}_israel_palestine_data.csv"
        df.to_csv(output_file, index=False)
        print(f"Données sauvegardées dans {output_file}")
