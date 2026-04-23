#!/bin/bash

# Détection de l'OS pour le fun (Optionnel)
OS_TYPE=$OSTYPE
echo " Système détecté : $OS_TYPE"

# Couleurs
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Vérification de Python
if ! command -v python3 &> /dev/null
then
    echo -e "${RED} Erreur : python3 n'est pas installé sur cette machine.${NC}"
    exit
fi

echo -e "${BLUE} Lancement du Pipeline...${NC}"

# Lancement des scripts avec python3
python3 main.py
python3 reporting.py
python3 visualizer.py

echo -e "${BLUE}✅ Terminé !${NC}"