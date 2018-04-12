On dispose d'un fichier CSV, selon le modèle suivant :

- `input.csv` : `userId,productId,rating,timestamp`

On souhaite construire 3 CSV de la façon suivante :

- `agg_ratings.csv` : `userIdAsInteger,productIdAsInteger,ratingSum`
- `lookup_user.csv` : `userId,userIdAsInteger`
- `lookup_product.csv` : `productId,productIdAsInteger`

Où :

- `userId` : identifiant unique d'un utilisateur (String)
- `productId` : identifiant unique d'un produit (String)
- `rating` : score (Float)
- `timestamp` : timestamp unix (Long)
- `userIdAsInteger` : identifiant unique d'un utilisateur (Int)
- `productIdAsInteger` : identifiant unique d'un produit (Int)
- `ratingSum` : Somme des ratings pour le couple utilisateur/produit (Float)

Écrire un programme Spark respectant les contraintes suivantes :

- Dans `agg_ratings`, les couples utilisateur/produit sont uniques.
- Les `userIdAsInteger` (tout comme les `productIdAsInteger`) sont des entiers consécutifs, le premier indice étant 0.
- Une pénalité multiplicative de `0.95` est appliquée au rating pour chaque jour d'écart avec le timestamp maximal de `input.csv`.
- On ne souhaite conserver que les `ratings > 0.01`.
