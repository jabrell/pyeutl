from codebase.entity_connexion import EntityConnexion
from attic.constants import EntityType

for i in range(1000):
    EntityConnexion(EntityType.Account, i)

# EntityConnexion('AccountHolder', 145)
# EntityConnexion('Company', 'FN 71396 w')
