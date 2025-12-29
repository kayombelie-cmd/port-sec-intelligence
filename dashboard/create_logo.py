import os
from PIL import Image, ImageDraw, ImageFont

# Créer le dossier assets
assets_dir = "assets"
if not os.path.exists(assets_dir):
    os.makedirs(assets_dir)

# Créer une image plus grande et plus détaillée
img = Image.new('RGBA', (1000, 1000), color=(0, 0, 0, 0))  # Fond transparent
draw = ImageDraw.Draw(img)

# --- FOND DÉGRADÉ ---
for i in range(300):
    # Dégradé de bleu marine (#1E3A8A) à bleu ciel (#3B82F6)
    r = int(30 + (59 - 30) * i / 300)
    g = int(58 + (130 - 58) * i / 300)
    b = int(138 + (246 - 138) * i / 300)
    draw.line([(0, i), (300, i)], fill=(r, g, b, 255))

# --- CONTENEURS EMPILÉS (style portuaire) ---
# Conteneur du bas
draw.rectangle([80, 150, 220, 200], fill='#FFD700', outline='#000000', width=2)  # Jaune
draw.rectangle([85, 155, 215, 195], fill='#FFA500', outline='#000000', width=1)

# Conteneur du milieu
draw.rectangle([90, 120, 210, 150], fill='#4682B4', outline='#000000', width=2)  # Acier
draw.rectangle([95, 125, 205, 145], fill='#5F9EA0', outline='#000000', width=1)

# Conteneur du haut
draw.rectangle([100, 90, 200, 120], fill='#32CD32', outline='#000000', width=2)  # Vert
draw.rectangle([105, 95, 195, 115], fill='#3CB371', outline='#000000', width=1)

# --- GRUE PORTUAIRE ---
# Base de la grue
draw.rectangle([145, 50, 155, 150], fill='#808080', outline='#000000', width=2)

# Flèche de la grue
draw.polygon([(150, 50), (130, 30), (170, 30)], fill='#DC143C', outline='#000000', width=2)

# Câble et crochet
draw.line([(150, 80), (150, 100)], fill='#000000', width=2)
draw.ellipse([145, 100, 155, 110], fill='#000000', outline='#000000', width=2)

# --- TEXTE "PSI" ÉLÉGANT ---
try:
    # Essayez de charger une police élégante, sinon police par défaut
    font = ImageFont.truetype("arialbd.ttf", 36)
except:
    try:
        font = ImageFont.truetype("arial.ttf", 36)
    except:
        font = ImageFont.load_default()

# Ombre du texte
draw.text((152, 202), "PSI", fill='#000000', font=font, anchor="mm")
# Texte principal
draw.text((150, 200), "PSI", fill='#FFFFFF', font=font, anchor="mm")

# --- BORDURE STYLÉE ---
draw.rectangle([5, 5, 295, 295], outline='#FFFFFF', width=4)
draw.rectangle([8, 8, 292, 292], outline='#1E3A8A', width=2)

# Sauvegarder
img.save("assets/logo.png")
print("✅ Logo professionnel créé : assets/logo.png")
print("   Taille : 300x300 pixels avec fond transparent")