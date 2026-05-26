# Reponses au TP - Spark Docker

Partie 1 — déploiement
- Master: 1
- Workers: 3
- Jupyter: 1
Architecture (très bref)
- Hôte: machine qui exécute Docker et contient `work/` et `stream-read/`.
- Conteneurs: `spark-master`, `spark-worker-*`, `spark-jupyter`.
Pandas
- Q1: Les données sont chargées en RAM du processus Python exécutant Pandas.
- Q2: Risque principal = manque de mémoire (OOM).
Limites de Pandas
- Q4: Pandas charge tout en mémoire; échoue si RAM insuffisante.
- Q5: Taille fichier → besoin RAM; Pandas = single-node, in-memory.
- Q6: Pas scalable (une seule machine).
- Q7: Pour 100 Go → utiliser Spark/Dask ou traitement par morceaux.

Spark (très bref)
- Créer `SparkSession` vers `spark://spark-master:7077`.
- Lire avec un `schema` si nécessaire.
- Opérations: `count()`, `select()`, `filter()`, `agg()`.
Questions conceptuelles
- Q8: Pandas = local/in-memory ; Spark = distribué/lazy.
- Q9: Spark gère plus de données via distribution et parallélisme.
- Q10: Workers exécutent des tâches sur des partitions.
- Q11: Spark = parallélisme, tolérance aux pannes.
- Q12: Inconvénients = overhead, complexité, besoin d'infra.

Comparaison courte
| Critère | Pandas | Spark |
|---|---:|---:|
| Chargement mémoire | RAM locale | partitionné |
| Calcul distribué | non | oui |
| Scalabilité | limitée | horizontale |
| Simplicité | simple | plus complexe |
Sorties observées (résumé)
- Conteneurs: `spark-master`, `spark-worker-1..3`, `spark-jupyter` → tous Up.
- Pandas: taille 0.0019 GB; shape (1137,16); read ~0.025 s; avg 4.488; >4.5 count = 797.
- Spark: v3.5.0; count = 1137; avg ~4.488; read ~16.9 s; partitions avant 1 après 8.
Fait dans `/workspaces/spark-docker`.
# Réponses TP — Spark Docker

Ce fichier contient les réponses concises aux manipulations et questions demandées dans le `README.md`, ainsi que les sorties observées lors de l'exécution des commandes.

---

## Partie 1 — Mise en place de l'environnement Spark

1.1 Architecture utilisée

- Spark Master : 1
- Spark Workers : 3
- Environnement Jupyter avec PySpark : 1

(Ces nombres proviennent du fichier `docker-compose.yml` fourni.)

1.3 Démarrage du cluster — sortie observée

- Commande exécutée : `docker compose up -d`
- Observation : le processus télécharge les images (pull) ; au moment de la vérification initiale aucun conteneur n'était encore en état `Up`.

Sortie de `docker ps` (au moment de l'observation) :

NAMES     STATUS    PORTS

(correspond à aucune ligne supplémentaire — aucun conteneur en `Up` au moment de la capture, car les images étaient encore en cours de pull)

> Note : le lancement a commencé (pull des images volumineuses : jupyter, spark workers, master). Selon la bande passante, le `docker compose up -d` peut prendre plusieurs minutes.

1.4 Vérification des dossiers montés

- Commande exécutée : création et permissions

  mkdir -p work stream-read && chmod -R 777 work stream-read && ls -la work stream-read

Sortie observée :

stream-read:
```
total 8
drwxrwxrwx+ 2 codespace codespace 4096 May 26 06:31 .
drwxrwxrwx+ 5 codespace root      4096 May 26 06:31 ..
```

work:
```
total 8
drwxrwxrwx+ 2 codespace codespace 4096 May 26 06:31 .
drwxrwxrwx+ 5 codespace root      4096 May 26 06:31 ..
```

Les dossiers existent et ont des permissions larges pour permettre au conteneur Jupyter/Spark d'y accéder.

Architecture (où se situe chaque élément)

- Hôte : machine locale qui exécute Docker (stocke les fichiers `work` et `stream-read` montés). 
- Fichiers : dans les dossiers montés `work/` et `stream-read/` sur l'hôte, accessibles depuis les conteneurs via les volumes Docker.
- Spark Master : conteneur `spark-master` orchestrant le cluster (driver UI/ressource manager).
- Spark Workers : conteneurs `spark-worker-1..3` exécutant les tâches et tenant la mémoire/CPU pour les partitions des données.
- Jupyter : conteneur Jupyter/PySpark qui lance les notebooks et peut créer une `SparkSession` pointant vers `spark://spark-master:7077`.

---

## Partie 3 — Premier test avec Pandas

Question 1 — Où les données sont-elles chargées lors de l’utilisation de Pandas ?

- Les données sont chargées en mémoire RAM du processus Python qui exécute Pandas (ici le container Jupyter si le code est lancé depuis le notebook). Pandas lit et construit des structures en mémoire (DataFrame) représentant l'ensemble du fichier.

Question 2 — Quel est le principal risque lorsque la taille des données augmente fortement ?

- Risque principal : dépassement de la mémoire (Out Of Memory), entraînant swap massif, ralentissement ou plantage du processus Python.

---

## Partie 4 — Test Pandas sur un très gros volume

Observations générales

- Pandas charge tout le fichier en mémoire avant d'opérer (pour pd.read_json avec lines=True), ce qui rend son usage limité par la RAM disponible.

Question 4 — Pourquoi Pandas rencontre-t-il cette situation ?

- Parce que Pandas est conçu pour travailler en mémoire sur une seule machine : il construit des objets Python/NumPy en RAM pour tout le dataset.

Question 5 — Lien entre taille du fichier, mémoire RAM et architecture de Pandas

- Taille du fichier : détermine l'ordre de grandeur de mémoire nécessaire.
- Mémoire RAM disponible : besoin de RAM >= taille des données (souvent plusieurs fois la taille en raison des structures intermédiaires et overhead).
- Architecture de Pandas : single-process, single-node, in-memory => limite intrinsèque liée à la RAM d'une seule machine.

Question 6 — Pourquoi cette approche devient-elle difficilement scalable ?

- Car augmenter la taille des données nécessite augmenter la RAM d'une seule machine (coûteux et limité), et Pandas ne distribue pas le calcul ni les données sur plusieurs nœuds.

Question 7 — Que faudrait-il faire si le fichier faisait 100 Go ?

- Solutions :
  - Utiliser un moteur distribué (Spark, Dask) qui partitionne les données et les calcule en parallèle sur plusieurs machines;
  - Traiter le fichier en streaming/chunks (itératif) si possible, ou pré-filtrer les données avant chargement complet;
  - Utiliser des instances avec beaucoup de RAM si le traitement doit rester sur une seule machine (moins recommandé pour 100+ Go).

---

## Partie 5 & 6 — Introduction à Spark et premières manipulations

(Résultats et commandes recommandées dans le README : création d'une SparkSession et lecture avec un schéma explicite.)

- Utiliser `SparkSession.builder.master("spark://spark-master:7077")...` depuis Jupyter pour se connecter au cluster.
- Lire avec un `schema` explicite pour éviter les erreurs dues à la qualité des données : `spark.read.format("json").schema(schema).load("work/meta_Automotive.jsonl")`.

Opérations possibles : `printSchema()`, `show()`, `count()`, `select(...)`, `filter(...)`, `agg(...)`.

---

## Partie 7 — Comprendre ce que fait Spark

Question 8 — Quelle différence fondamentale existe-t-il entre Pandas et Spark ?

- Pandas : traitement en mémoire sur une seule machine (single-node, eager evaluation). 
- Spark : moteur distribué (multi-node), évaluation paresseuse (lazy), partitionne les données et exécute des tâches en parallèle sur plusieurs workers.

Question 9 — Pourquoi Spark peut-il traiter des volumes beaucoup plus importants ?

- Parce qu'il répartit les données et le calcul sur plusieurs nœuds, agrège la mémoire/CPU et peut persister sur disque/RDD/DataFrame partitions; il optimise aussi les plans d'exécution.

Question 10 — Quel est le rôle des workers dans Spark ?

- Exécuter les tasks (opérations sur des partitions de données), stocker les partitions en mémoire/disk, rapporter les résultats au driver/master.

Question 11 — Pourquoi Spark est-il adapté au Big Data ?

- Distribution des données, parallélisme, tolérance aux pannes (réexécution des partitions), optimisations (Catalyst, Tungsten), intégration avec stockage distribué.

Question 12 — Quels sont les inconvénients possibles de Spark ?

- Surcharge pour petits fichiers (latence, overhead de planification), complexité d'exploitation, consommation de ressources, coût d'infrastructure, courbe d'apprentissage.

---

## Partie 8/9 — Choix et cas d'usage

Question 13 — Dans quels cas Pandas reste-t-il un très bon choix ?

- Petits jeux de données (qui tiennent en RAM), exploration interactive, prototypage, analyses ad-hoc locales.

Question 14 — Dans quels cas Spark devient-il indispensable ?

- Données volumineuses (tens/ hundreds GB / TB), besoin de parallélisme/distribution, pipeline de production nécessitant scalabilité et tolérance aux pannes.

Question 15 — Pourquoi le calcul distribué devient essentiel dans les architectures Big Data modernes ?

- Permet de gérer des volumes bien supérieurs à la mémoire d'une seule machine, d'accélérer les traitements par parallélisme et d'améliorer la résilience et la disponibilité.

Question 16 — Pourquoi plusieurs machines peuvent être plus efficaces qu’une seule machine très puissante ?

- Agrégation de mémoire et CPU, meilleur débit I/O (parallélisme disque/réseau), coût potentiellement inférieur par rapport à une machine extrême, résilience (panne d'un nœud n'arrête pas tout).

---

## Comparaison Pandas vs Spark

| Critère                      | Pandas                                | Spark                                          |
|-----------------------------:|---------------------------------------:|-----------------------------------------------:|
| Chargement mémoire           | En mémoire sur une seule machine      | Partitionné et distribué                       |
| Calcul distribué             | Non                                   | Oui                                            |
| Scalabilité                  | Limitée à la RAM/CPU de la machine    | Horizontale (ajout de workers)                 |
| Simplicité d’utilisation     | Très simple et interactif              | Plus complexe (cluster, configuration)        |
| Performances petits fichiers | Excellente                            | Correct (overhead de planification)            |
| Performances gros volumes    | Mauvaise (OOM)                        | Excellente (parallélisme, IO optimisé)        |
| Tolérance aux pannes         | Aucune                                | Réexécution des partitions, résilience        |
| Infrastructure nécessaire    | Une seule machine                     | Cluster (master + workers)                     |


## Sortie des commandes exécutées (résumé)

- `docker compose up -d` : pull des images en cours (jupyter et spark images volumineuses). Au moment de la capture, aucun conteneur `Up` parce que le pull n'était pas terminé.
- `docker ps` : aucune ligne de conteneur en `Up` lors de la vérification initiale.
- Création et permissions des dossiers : `work` et `stream-read` créés avec `chmod -R 777`.

### Sorties réelles observées

- État des conteneurs (`docker ps`):

```
NAMES            STATUS                   PORTS
spark-worker-3   Up 4 minutes             
spark-worker-1   Up 4 minutes             
spark-worker-2   Up 4 minutes             
spark-jupyter    Up 4 minutes (healthy)   0.0.0.0:4040->4040/tcp, 0.0.0.0:8888->8888/tcp
spark-master     Up 4 minutes             0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
```

- Test Pandas (exécuté dans le conteneur `spark-jupyter` sur `/home/jovyan/work/meta_Gift_Cards.jsonl`):
 - Test Pandas (exécuté dans le conteneur `spark-jupyter` sur `/home/jovyan/work/meta_Gift_Cards.jsonl`):

```
Taille du fichier : 0.0019 GB
Temps lecture Pandas: 0.0247 s
Pandas dataframe shape: (1137, 16)
Pandas average_rating (moyenne): 4.4883905013192615
Pandas count average_rating > 4.5: 797
(affichage 3 premières lignes inclus dans le log)
```

- Test Spark (lancé via `spark-submit` dans le conteneur `spark-master` sur `/home/jovyan/work/spark_test.py`):
 - Test Spark (lancé via `spark-submit` dans le conteneur `spark-master`):

```
Spark version: 3.5.0
(script `/home/jovyan/work/spark_test.py`) Count: 1137
Schéma: colonnes principales (author, average_rating, categories, description, details, features, images, main_category, parent_asin, price, rating_number, store, subtitle, title, videos)
Show 5 rows: affiché (résumé dans le log)
```

Résultats des opérations Spark détaillées (script `/home/jovyan/work/spark_ops.py`):

```
spark_read_time: 16.90519094467163 s
count: 1137
select title, average_rating (top 10): affiché (voir log)
filter average_rating > 4.5 (top 10): affiché (voir log)
average rating (spark): 4.488390501319281
num_partitions_before: 1
num_partitions_after_repartition_call (logical): 8
total_job_time: 29.70549726486206 s
```
