"""Build a theological lexicon from existing entities and CCC definitions.

Seeds ~100-200 terms with multilingual names, etymologies, definitions,
and links to CCC paragraphs. Sources:
  - 89 existing entities (names + categories + paragraph links)
  - Known Latin/Greek theological terminology
  - CCC paragraph text (for brief definitions)

Usage:
    python -m pipeline.src.build_lexicon [--dry-run]
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "knowledge-graph.db"


# ---------------------------------------------------------------------------
# Theological term data
# ---------------------------------------------------------------------------

# Each entry: (entity_id or new_id, label_en, label_la, label_el, etymology, definition, category)
# entity_id matches existing entities table; new terms have fresh IDs.

LEXICON_TERMS: list[dict] = [
    # --- Trinitarian ---
    {"id": "trinity", "en": "Trinity", "la": "Trinitas", "el": "Τριάς",
     "etymology": "Latin 'trinitas' (threefold), from 'trinus' (triple). Coined by Tertullian (c. 200 AD).",
     "definition": "The central mystery of Christian faith: one God in three divine Persons — Father, Son, and Holy Spirit — distinct yet consubstantial.",
     "category": "trinitarian"},
    {"id": "god-father", "en": "God the Father", "la": "Deus Pater", "el": "Θεὸς Πατήρ",
     "etymology": "Latin 'pater' (father), Greek 'patḗr' (πατήρ). Biblical title from Jesus' own usage (Abba).",
     "definition": "The First Person of the Trinity, the unbegotten source and origin of the Son and the Holy Spirit.",
     "category": "trinitarian"},
    {"id": "god-son", "en": "God the Son", "la": "Deus Filius", "el": "Θεὸς Υἱός",
     "etymology": "Latin 'filius' (son), Greek 'huios' (υἱός). The eternal Word (Logos) begotten of the Father.",
     "definition": "The Second Person of the Trinity, eternally begotten of the Father, who became incarnate as Jesus Christ.",
     "category": "trinitarian"},
    {"id": "holy-spirit", "en": "Holy Spirit", "la": "Spiritus Sanctus", "el": "Ἅγιον Πνεῦμα",
     "etymology": "Latin 'spiritus' (breath, spirit), Greek 'pneuma' (πνεῦμα, breath/wind). Hebrew 'ruach' (רוּחַ).",
     "definition": "The Third Person of the Trinity, who proceeds from the Father and the Son, the Lord and Giver of life.",
     "category": "trinitarian"},
    {"id": "consubstantial", "en": "Consubstantial", "la": "Consubstantialis", "el": "Ὁμοούσιος",
     "etymology": "Greek 'homoousios' (ὁμοούσιος, of the same substance), from 'homos' (same) + 'ousia' (being/substance). Defined at Nicaea (325).",
     "definition": "Of the same substance or essence. Used to affirm that the Son is of the same divine nature as the Father.",
     "category": "trinitarian"},
    {"id": "hypostasis", "en": "Hypostasis", "la": "Subsistentia", "el": "Ὑπόστασις",
     "etymology": "Greek 'hypostasis' (ὑπόστασις, underlying reality), from 'hypo' (under) + 'stasis' (standing). Cappadocian Fathers distinguished it from 'ousia'.",
     "definition": "A distinct subsistence or person within the Godhead. The Trinity is one ousia (essence) in three hypostases (persons).",
     "category": "trinitarian"},
    {"id": "ousia", "en": "Essence/Substance", "la": "Essentia / Substantia", "el": "Οὐσία",
     "etymology": "Greek 'ousia' (οὐσία, being/essence), from the participle of 'einai' (to be). Latin equivalent 'substantia' from 'sub' (under) + 'stare' (to stand).",
     "definition": "The divine nature or being shared by all three Persons of the Trinity. One ousia, three hypostases.",
     "category": "trinitarian"},

    # --- Christology ---
    {"id": "messiah", "en": "Messiah / Christ", "la": "Christus", "el": "Χριστός / Μεσσίας",
     "etymology": "Greek 'Christos' (Χριστός, anointed one), translating Hebrew 'Mashiach' (מָשִׁיחַ). Latin 'Christus' borrowed from Greek.",
     "definition": "The Anointed One of God, the promised deliverer of Israel. Christians confess Jesus of Nazareth as the Christ.",
     "category": "christology"},
    {"id": "incarnation", "en": "Incarnation", "la": "Incarnatio", "el": "Ἐνσάρκωσις",
     "etymology": "Latin 'incarnatio' (becoming flesh), from 'in' + 'caro/carnis' (flesh). Greek 'ensarkōsis' from 'en' (in) + 'sarx' (σάρξ, flesh).",
     "definition": "The mystery of the Word made flesh: the Son of God assumed a human nature in order to accomplish salvation.",
     "category": "christology"},
    {"id": "resurrection", "en": "Resurrection", "la": "Resurrectio", "el": "Ἀνάστασις",
     "etymology": "Latin 'resurrectio' (rising again), from 're-' (again) + 'surgere' (to rise). Greek 'anastasis' (ἀνάστασις) from 'ana' (up) + 'stasis' (standing).",
     "definition": "Christ's bodily rising from the dead on the third day, the foundation of Christian faith and the pledge of our own resurrection.",
     "category": "christology"},
    {"id": "redemption", "en": "Redemption", "la": "Redemptio", "el": "Ἀπολύτρωσις",
     "etymology": "Latin 'redemptio' (buying back), from 're-' (back) + 'emere' (to buy). Greek 'apolytrōsis' (ἀπολύτρωσις) from 'apo' (from) + 'lytron' (ransom).",
     "definition": "The act of Christ freeing humanity from sin and death through his Passion, Death, and Resurrection.",
     "category": "christology"},
    {"id": "kenosis", "en": "Kenosis", "la": "Exinanitio", "el": "Κένωσις",
     "etymology": "Greek 'kenōsis' (κένωσις, emptying), from 'kenoun' (to empty). From Philippians 2:7: Christ 'emptied himself' (ἑαυτὸν ἐκένωσεν).",
     "definition": "The self-emptying of the Son of God in the Incarnation, taking the form of a servant while remaining divine.",
     "category": "christology"},
    {"id": "logos", "en": "Logos / Word", "la": "Verbum", "el": "Λόγος",
     "etymology": "Greek 'logos' (λόγος, word/reason/principle). In Greek philosophy: cosmic reason. In John 1:1: the eternal Word of God.",
     "definition": "The Second Person of the Trinity as the eternal Word of God, through whom all things were made (John 1:1-3).",
     "category": "christology"},
    {"id": "parousia", "en": "Parousia", "la": "Adventus", "el": "Παρουσία",
     "etymology": "Greek 'parousia' (παρουσία, presence/arrival), from 'para' (beside) + 'ousia' (being). Used in NT for Christ's second coming.",
     "definition": "The Second Coming of Christ in glory at the end of time to judge the living and the dead.",
     "category": "eschatology"},

    # --- Soteriology ---
    {"id": "grace", "en": "Grace", "la": "Gratia", "el": "Χάρις",
     "etymology": "Latin 'gratia' (favor, thanks), from 'gratus' (pleasing). Greek 'charis' (χάρις, grace/gift/favor).",
     "definition": "The free and undeserved help God gives us to respond to his call and to live as his adopted children.",
     "category": "soteriology"},
    {"id": "salvation", "en": "Salvation", "la": "Salus", "el": "Σωτηρία",
     "etymology": "Latin 'salus' (health, safety, salvation), from 'salvare' (to save). Greek 'sōtēria' (σωτηρία) from 'sōzein' (to save).",
     "definition": "Deliverance from sin and its consequences, brought about by God's grace through faith in Jesus Christ.",
     "category": "soteriology"},
    {"id": "justification", "en": "Justification", "la": "Iustificatio", "el": "Δικαίωσις",
     "etymology": "Latin 'iustificatio' (making just), from 'iustus' (just) + 'facere' (to make). Greek 'dikaiōsis' (δικαίωσις).",
     "definition": "The gracious action of God that frees us from sin and communicates the righteousness of God through faith in Christ.",
     "category": "soteriology"},
    {"id": "original-sin", "en": "Original Sin", "la": "Peccatum Originale", "el": "Προπατορικὸν Ἁμάρτημα",
     "etymology": "Latin 'peccatum originale' (sin of origin). Term systematized by Augustine against the Pelagians (5th century).",
     "definition": "The fallen state of human nature inherited from Adam's first sin, depriving humanity of original holiness and justice.",
     "category": "soteriology"},
    {"id": "theosis", "en": "Theosis / Divinization", "la": "Deificatio", "el": "Θέωσις",
     "etymology": "Greek 'theōsis' (θέωσις, deification), from 'theos' (God). Athanasius: 'God became man so that man might become god.'",
     "definition": "The transforming effect of grace whereby humans become partakers of the divine nature (2 Peter 1:4).",
     "category": "soteriology"},

    # --- Sacraments ---
    {"id": "sacrament", "en": "Sacrament", "la": "Sacramentum", "el": "Μυστήριον",
     "etymology": "Latin 'sacramentum' (sacred oath, mystery), originally a military oath. Greek 'mystērion' (μυστήριον, mystery/secret rite).",
     "definition": "An efficacious sign of grace, instituted by Christ and entrusted to the Church, by which divine life is dispensed to us.",
     "category": "sacraments"},
    {"id": "eucharist", "en": "Eucharist", "la": "Eucharistia", "el": "Εὐχαριστία",
     "etymology": "Greek 'eucharistia' (εὐχαριστία, thanksgiving), from 'eu' (good) + 'charis' (grace/favor). Jesus 'gave thanks' at the Last Supper.",
     "definition": "The sacrament of Christ's Body and Blood under the species of bread and wine, the source and summit of Christian life.",
     "category": "sacraments"},
    {"id": "transubstantiation", "en": "Transubstantiation", "la": "Transsubstantiatio", "el": "Μετουσίωσις",
     "etymology": "Latin 'transsubstantiatio' (change of substance), from 'trans' (across) + 'substantia' (substance). Term adopted at Lateran IV (1215).",
     "definition": "The change of the whole substance of bread and wine into the Body and Blood of Christ, while the appearances remain.",
     "category": "sacraments"},
    {"id": "baptism", "en": "Baptism", "la": "Baptismus", "el": "Βάπτισμα",
     "etymology": "Greek 'baptisma' (βάπτισμα, immersion), from 'baptein' (to dip/immerse). Latin 'baptismus' borrowed from Greek.",
     "definition": "The first sacrament of initiation, which forgives original and personal sin and incorporates into Christ and his Church.",
     "category": "sacraments"},
    {"id": "reconciliation", "en": "Reconciliation / Penance", "la": "Paenitentia", "el": "Μετάνοια",
     "etymology": "Greek 'metanoia' (μετάνοια, change of mind/repentance), from 'meta' (change) + 'nous' (mind). Latin 'paenitentia' from 'paenitere' (to repent).",
     "definition": "The sacrament by which sins committed after Baptism are forgiven through the priest's absolution.",
     "category": "sacraments"},
    {"id": "holy-orders", "en": "Holy Orders", "la": "Ordo", "el": "Χειροτονία",
     "etymology": "Latin 'ordo' (order, rank). Greek 'cheirotonia' (χειροτονία, laying on of hands), from 'cheir' (hand) + 'teinein' (to stretch).",
     "definition": "The sacrament through which the mission entrusted by Christ to his apostles continues to be exercised in the Church through bishops, priests, and deacons.",
     "category": "sacraments"},
    {"id": "matrimony", "en": "Matrimony", "la": "Matrimonium", "el": "Γάμος",
     "etymology": "Latin 'matrimonium' (marriage), from 'mater' (mother) + '-monium' (state/condition). Greek 'gamos' (γάμος, wedding).",
     "definition": "The sacramental covenant between a baptized man and woman, by which they establish a lifelong partnership ordered to the good of the spouses and the procreation of children.",
     "category": "sacraments"},
    {"id": "confirmation", "en": "Confirmation", "la": "Confirmatio", "el": "Χρίσμα",
     "etymology": "Latin 'confirmatio' (strengthening). Greek 'chrisma' (χρίσμα, anointing), from 'chriein' (to anoint), same root as 'Christos'.",
     "definition": "The sacrament that completes baptismal grace, conferring the Holy Spirit to strengthen the Christian for witness and spiritual combat.",
     "category": "sacraments"},
    {"id": "anointing-sick", "en": "Anointing of the Sick", "la": "Unctio Infirmorum", "el": "Εὐχέλαιον",
     "etymology": "Latin 'unctio' (anointing) + 'infirmorum' (of the sick). Greek 'euchelaion' (εὐχέλαιον) from 'euchē' (prayer) + 'elaion' (oil).",
     "definition": "The sacrament that gives the grace of the Holy Spirit to those suffering from serious illness or old age, uniting their suffering to Christ's Passion.",
     "category": "sacraments"},

    # --- Ecclesiology ---
    {"id": "church", "en": "Church", "la": "Ecclesia", "el": "Ἐκκλησία",
     "etymology": "Greek 'ekklēsia' (ἐκκλησία, assembly), from 'ek' (out) + 'kalein' (to call). Latin 'ecclesia' borrowed from Greek. The 'called-out' assembly.",
     "definition": "The community of all baptized believers united in faith, constituted as the Body of Christ and the People of God.",
     "category": "ecclesiology"},
    {"id": "episcopate", "en": "Bishop / Episcopate", "la": "Episcopatus", "el": "Ἐπίσκοπος",
     "etymology": "Greek 'episkopos' (ἐπίσκοπος, overseer), from 'epi' (over) + 'skopein' (to look). Latin 'episcopus'.",
     "definition": "The fullness of the sacrament of Holy Orders. Bishops are successors of the apostles, governing particular churches.",
     "category": "ecclesiology"},
    {"id": "apostolic-succession", "en": "Apostolic Succession", "la": "Successio Apostolica", "el": "Ἀποστολικὴ Διαδοχή",
     "etymology": "Latin 'successio' (following after) + 'apostolica' (of the apostles). The unbroken chain of episcopal ordination from the apostles.",
     "definition": "The uninterrupted transmission of episcopal authority from the Apostles through successive bishops by the laying on of hands.",
     "category": "ecclesiology"},
    {"id": "magisterium", "en": "Magisterium", "la": "Magisterium", "el": "Διδασκαλία",
     "etymology": "Latin 'magisterium' (teaching authority), from 'magister' (teacher/master). The teaching office of the Church.",
     "definition": "The living teaching office of the Church, exercised by the Pope and bishops in communion with him, to authentically interpret the Word of God.",
     "category": "ecclesiology"},

    # --- Mariology ---
    {"id": "theotokos", "en": "Theotokos / Mother of God", "la": "Deipara / Mater Dei", "el": "Θεοτόκος",
     "etymology": "Greek 'Theotokos' (Θεοτόκος, God-bearer), from 'Theos' (God) + 'tokos' (birth/offspring). Defined at Ephesus (431).",
     "definition": "Title of Mary affirming that she is truly the Mother of God, since the child she bore is the divine Person of the Son.",
     "category": "mariology"},
    {"id": "immaculate-conception", "en": "Immaculate Conception", "la": "Immaculata Conceptio", "el": "Ἄσπιλος Σύλληψις",
     "etymology": "Latin 'immaculata' (unstained) + 'conceptio' (conception). Dogma proclaimed by Pius IX in 1854 (Ineffabilis Deus).",
     "definition": "The privilege by which Mary was preserved free from original sin from the first moment of her conception, by God's grace.",
     "category": "mariology"},
    {"id": "assumption", "en": "Assumption", "la": "Assumptio", "el": "Κοίμησις",
     "etymology": "Latin 'assumptio' (taking up). Greek 'Koimēsis' (Κοίμησις, falling asleep/Dormition). Dogma proclaimed by Pius XII in 1950.",
     "definition": "The bodily taking up of the Virgin Mary into heavenly glory at the end of her earthly life.",
     "category": "mariology"},

    # --- Virtues ---
    {"id": "faith", "en": "Faith", "la": "Fides", "el": "Πίστις",
     "etymology": "Latin 'fides' (trust, belief, loyalty). Greek 'pistis' (πίστις, faith/trust), from 'peithein' (to persuade).",
     "definition": "The theological virtue by which we believe in God and all that he has revealed, as the Church proposes for our belief.",
     "category": "virtues"},
    {"id": "hope", "en": "Hope", "la": "Spes", "el": "Ἐλπίς",
     "etymology": "Latin 'spes' (hope, expectation). Greek 'elpis' (ἐλπίς, hope/expectation).",
     "definition": "The theological virtue by which we desire and expect from God both eternal life and the grace needed to attain it.",
     "category": "virtues"},
    {"id": "charity", "en": "Charity / Love", "la": "Caritas", "el": "Ἀγάπη",
     "etymology": "Latin 'caritas' (dearness, love), from 'carus' (dear). Greek 'agapē' (ἀγάπη, selfless love). Distinct from 'eros' and 'philia'.",
     "definition": "The theological virtue by which we love God above all things and our neighbor as ourselves for the love of God. The greatest of the virtues.",
     "category": "virtues"},
    {"id": "justice", "en": "Justice", "la": "Iustitia", "el": "Δικαιοσύνη",
     "etymology": "Latin 'iustitia' (righteousness), from 'ius' (right/law). Greek 'dikaiosynē' (δικαιοσύνη), from 'dikaios' (just).",
     "definition": "The cardinal virtue of rendering to each person what is due. In Scripture, also the righteousness of God's saving action.",
     "category": "virtues"},
    {"id": "prudence", "en": "Prudence", "la": "Prudentia", "el": "Φρόνησις",
     "etymology": "Latin 'prudentia' (foresight), contracted from 'providentia'. Greek 'phronēsis' (φρόνησις, practical wisdom).",
     "definition": "The cardinal virtue that disposes practical reason to discern the true good in every circumstance and to choose the right means of achieving it.",
     "category": "virtues"},
    {"id": "temperance", "en": "Temperance", "la": "Temperantia", "el": "Σωφροσύνη",
     "etymology": "Latin 'temperantia' (moderation), from 'temperare' (to mix properly). Greek 'sōphrosynē' (σωφροσύνη, sound-mindedness).",
     "definition": "The cardinal virtue that moderates the attraction of pleasures and provides balance in the use of created goods.",
     "category": "virtues"},
    {"id": "fortitude", "en": "Fortitude", "la": "Fortitudo", "el": "Ἀνδρεία",
     "etymology": "Latin 'fortitudo' (strength, courage), from 'fortis' (strong). Greek 'andreia' (ἀνδρεία, manliness/courage).",
     "definition": "The cardinal virtue that ensures firmness in difficulties and constancy in the pursuit of the good.",
     "category": "virtues"},

    # --- Revelation & Scripture ---
    {"id": "sacred-scripture", "en": "Sacred Scripture", "la": "Sacra Scriptura", "el": "Ἁγία Γραφή",
     "etymology": "Latin 'scriptura' (writing), from 'scribere' (to write). Greek 'graphē' (γραφή, writing/scripture).",
     "definition": "The books of the Old and New Testaments, written under the inspiration of the Holy Spirit, having God as their author.",
     "category": "revelation"},
    {"id": "sacred-tradition", "en": "Sacred Tradition", "la": "Sacra Traditio", "el": "Ἱερὰ Παράδοσις",
     "etymology": "Latin 'traditio' (handing over/down), from 'tradere' (to hand over). Greek 'paradosis' (παράδοσις, tradition).",
     "definition": "The living transmission of the Word of God entrusted to the apostles, handed on in the Church's teaching, life, and worship.",
     "category": "revelation"},
    {"id": "revelation", "en": "Divine Revelation", "la": "Revelatio Divina", "el": "Ἀποκάλυψις",
     "etymology": "Latin 'revelatio' (unveiling), from 're-' (back) + 'velare' (to veil). Greek 'apokalypsis' (ἀποκάλυψις, uncovering).",
     "definition": "God's self-communication to humanity, by which he makes known the mystery of his divine plan through deeds and words.",
     "category": "revelation"},

    # --- Liturgy ---
    {"id": "liturgy", "en": "Liturgy", "la": "Liturgia", "el": "Λειτουργία",
     "etymology": "Greek 'leitourgia' (λειτουργία, public service), from 'leitos' (public) + 'ergon' (work). Originally: public works for the people.",
     "definition": "The public worship of the Church, the participation of the People of God in the work of God, especially the Eucharistic celebration.",
     "category": "liturgy"},
    {"id": "anamnesis", "en": "Anamnesis", "la": "Anamnesis", "el": "Ἀνάμνησις",
     "etymology": "Greek 'anamnēsis' (ἀνάμνησις, remembrance), from 'ana' (again) + 'mimnēskein' (to remember). 'Do this in remembrance of me' (Luke 22:19).",
     "definition": "The liturgical memorial that makes present the saving events of Christ, not mere recollection but real re-presentation.",
     "category": "liturgy"},
    {"id": "epiclesis", "en": "Epiclesis", "la": "Epiclesis", "el": "Ἐπίκλησις",
     "etymology": "Greek 'epiklēsis' (ἐπίκλησις, invocation), from 'epi' (upon) + 'kalein' (to call).",
     "definition": "The invocation of the Holy Spirit upon the eucharistic offerings to transform them into the Body and Blood of Christ.",
     "category": "liturgy"},

    # --- Eschatology ---
    {"id": "heaven", "en": "Heaven", "la": "Caelum", "el": "Οὐρανός",
     "etymology": "Latin 'caelum' (sky, heaven). Greek 'ouranos' (οὐρανός, heaven). In Scripture: God's dwelling and the state of eternal beatitude.",
     "definition": "The ultimate end and fulfillment of the deepest human longings: the state of supreme, definitive happiness with God forever.",
     "category": "eschatology"},
    {"id": "purgatory", "en": "Purgatory", "la": "Purgatorium", "el": "Καθαρτήριον",
     "etymology": "Latin 'purgatorium' (place of cleansing), from 'purgare' (to cleanse/purify).",
     "definition": "The state of final purification after death for those who die in God's grace but are not yet fully purified.",
     "category": "eschatology"},
    {"id": "eschatology", "en": "Eschatology", "la": "Eschatologia", "el": "Ἐσχατολογία",
     "etymology": "Greek 'eschatos' (ἔσχατος, last) + 'logos' (word/study). The study of the 'last things'.",
     "definition": "The area of theology concerning the final destiny of the soul, of humanity, and of the world: death, judgment, heaven, and hell.",
     "category": "eschatology"},

    # --- Anthropology ---
    {"id": "soul", "en": "Soul", "la": "Anima", "el": "Ψυχή",
     "etymology": "Latin 'anima' (breath, soul, life). Greek 'psychē' (ψυχή, soul/life), from 'psychein' (to breathe).",
     "definition": "The spiritual principle in the human person, created directly by God, immortal, and the form of the body.",
     "category": "anthropology"},
    {"id": "creation", "en": "Creation", "la": "Creatio", "el": "Κτίσις",
     "etymology": "Latin 'creatio' (making/producing), from 'creare' (to produce/create). Greek 'ktisis' (κτίσις, creation/creature).",
     "definition": "God's act of bringing all things into existence from nothing (ex nihilo), and the totality of what God has created.",
     "category": "anthropology"},
    {"id": "free-will", "en": "Free Will", "la": "Liberum Arbitrium", "el": "Αὐτεξούσιον",
     "etymology": "Latin 'liberum arbitrium' (free judgment). Greek 'autexousion' (αὐτεξούσιον, self-determination), from 'autos' (self) + 'exousia' (power/authority).",
     "definition": "The power rooted in reason and will to act or not to act, to do this or that, and so to perform deliberate actions on one's own responsibility.",
     "category": "anthropology"},
    {"id": "conscience", "en": "Conscience", "la": "Conscientia", "el": "Συνείδησις",
     "etymology": "Latin 'conscientia' (shared knowledge, moral sense), from 'con-' (with) + 'scire' (to know). Greek 'syneidēsis' (συνείδησις).",
     "definition": "A judgment of reason whereby the human person recognizes the moral quality of a concrete act.",
     "category": "moral"},
    {"id": "natural-law", "en": "Natural Law", "la": "Lex Naturalis", "el": "Φυσικὸς Νόμος",
     "etymology": "Latin 'lex naturalis' (law of nature). Aquinas: participation of the rational creature in the eternal law of God.",
     "definition": "The moral law inscribed in the human heart by the Creator, discernible by reason, expressing the dignity of the human person.",
     "category": "moral"},

    # --- Prayer ---
    {"id": "lords-prayer", "en": "Lord's Prayer", "la": "Oratio Dominica / Pater Noster", "el": "Κυριακὴ Προσευχή",
     "etymology": "Latin 'Pater Noster' (Our Father), opening words of the prayer. Given by Jesus himself (Matthew 6:9-13, Luke 11:2-4).",
     "definition": "The prayer Jesus taught his disciples, the summary of the whole Gospel and the most perfect of prayers.",
     "category": "prayer"},

    # --- Additional important terms ---
    {"id": "catechesis", "en": "Catechesis", "la": "Catechesis", "el": "Κατήχησις",
     "etymology": "Greek 'katēchēsis' (κατήχησις, oral instruction), from 'kata' (down) + 'ēchein' (to sound). Teaching by word of mouth.",
     "definition": "The systematic instruction in the faith given to candidates for Baptism and to the faithful for their ongoing formation.",
     "category": "ecclesiology"},
    {"id": "dogma", "en": "Dogma", "la": "Dogma", "el": "Δόγμα",
     "etymology": "Greek 'dogma' (δόγμα, opinion/decree), from 'dokein' (to seem/think). In Church usage: a truth solemnly defined.",
     "definition": "A truth contained in divine Revelation, proposed by the Church for belief as divinely revealed, either by solemn judgment or ordinary Magisterium.",
     "category": "revelation"},
    {"id": "economy-salvation", "en": "Economy of Salvation", "la": "Oeconomia Salutis", "el": "Οἰκονομία τῆς Σωτηρίας",
     "etymology": "Greek 'oikonomia' (οἰκονομία, household management), from 'oikos' (house) + 'nomos' (law). God's 'plan' or 'arrangement' for salvation.",
     "definition": "God's plan for bringing all humanity to share in divine life, unfolding through creation, covenant, incarnation, and the Church.",
     "category": "soteriology"},
    {"id": "sensus-fidei", "en": "Sense of the Faith", "la": "Sensus Fidei", "el": "Αἴσθησις τῆς Πίστεως",
     "etymology": "Latin 'sensus fidei' (sense/instinct of faith). The supernatural appreciation of the faith by the whole People of God.",
     "definition": "The supernatural sense of faith of the whole People of God, by which the Church as a whole cannot err in matters of belief.",
     "category": "ecclesiology"},
    {"id": "ex-nihilo", "en": "Creation ex nihilo", "la": "Creatio ex Nihilo", "el": "Δημιουργία ἐξ οὐκ ὄντων",
     "etymology": "Latin 'ex nihilo' (out of nothing). The doctrine that God created the world without pre-existing matter. Cf. 2 Maccabees 7:28.",
     "definition": "The doctrine that God created all things from nothing, without any pre-existing material, by his Word alone.",
     "category": "anthropology"},
    {"id": "filioque", "en": "Filioque", "la": "Filioque", "el": "Καὶ ἐκ τοῦ Υἱοῦ",
     "etymology": "Latin 'filioque' (and from the Son). Added to the Nicene Creed in the West to affirm the Spirit's procession from both Father and Son.",
     "definition": "The Latin addition to the Nicene Creed stating that the Holy Spirit proceeds from the Father 'and the Son,' a point of contention between East and West.",
     "category": "trinitarian"},
    {"id": "communion-saints", "en": "Communion of Saints", "la": "Communio Sanctorum", "el": "Κοινωνία τῶν Ἁγίων",
     "etymology": "Latin 'communio sanctorum' can mean both 'communion of holy persons' and 'sharing in holy things.' Both senses apply.",
     "definition": "The spiritual solidarity which binds together the faithful on earth, the souls in purgatory, and the saints in heaven.",
     "category": "ecclesiology"},
]


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def create_tables(conn: sqlite3.Connection) -> None:
    """Create lexicon tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lexicon (
            id         TEXT PRIMARY KEY,
            term_en    TEXT NOT NULL,
            term_la    TEXT,
            term_el    TEXT,
            etymology  TEXT,
            definition TEXT,
            category   TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lexicon_paragraphs (
            term_id      TEXT NOT NULL,
            paragraph_id INTEGER NOT NULL,
            PRIMARY KEY (term_id, paragraph_id),
            FOREIGN KEY (term_id) REFERENCES lexicon(id),
            FOREIGN KEY (paragraph_id) REFERENCES paragraphs(id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lexicon_paragraphs_term
        ON lexicon_paragraphs(term_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lexicon_paragraphs_para
        ON lexicon_paragraphs(paragraph_id)
    """)


def load_terms(conn: sqlite3.Connection, dry_run: bool) -> int:
    """Insert lexicon terms and link to paragraphs via existing entities."""
    count = 0
    for term in LEXICON_TERMS:
        if not dry_run:
            conn.execute("""
                INSERT OR REPLACE INTO lexicon (id, term_en, term_la, term_el, etymology, definition, category)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (term["id"], term["en"], term.get("la", ""), term.get("el", ""),
                  term.get("etymology", ""), term.get("definition", ""), term.get("category", "")))

        # Link to paragraphs via existing entity mappings
        para_rows = conn.execute(
            "SELECT paragraph_id FROM paragraph_entities WHERE entity_id = ?",
            (term["id"],)
        ).fetchall()

        for row in para_rows:
            if not dry_run:
                conn.execute("""
                    INSERT OR IGNORE INTO lexicon_paragraphs (term_id, paragraph_id)
                    VALUES (?, ?)
                """, (term["id"], row[0]))

        count += 1

    return count


def build_lexicon_fts(conn: sqlite3.Connection) -> None:
    """Build FTS index for the lexicon."""
    conn.execute("DROP TABLE IF EXISTS lexicon_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE lexicon_fts USING fts5(
            id, term_en, term_la, term_el, etymology, definition, category
        )
    """)
    conn.execute("""
        INSERT INTO lexicon_fts
        SELECT id, term_en, term_la, term_el, etymology, definition, category
        FROM lexicon
    """)
    count = conn.execute("SELECT COUNT(*) FROM lexicon_fts").fetchone()[0]
    click.echo(f"  Lexicon FTS built with {count} rows.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--dry-run", is_flag=True, help="Show what would be done")
@click.option("--db", type=click.Path(), default=None, help="Path to database")
def main(dry_run: bool, db: str | None) -> None:
    """Build the theological lexicon from entity data and known terms."""
    db_path = Path(db) if db else DB_PATH
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    click.echo("=== Building Theological Lexicon ===")

    if not dry_run:
        create_tables(conn)

    count = load_terms(conn, dry_run)

    # Count paragraph links
    if not dry_run:
        link_count = conn.execute("SELECT COUNT(*) FROM lexicon_paragraphs").fetchone()[0]
    else:
        link_count = 0

    if not dry_run:
        conn.commit()
        build_lexicon_fts(conn)
        conn.commit()

    conn.close()

    suffix = " (dry-run)" if dry_run else ""
    click.echo(f"\nDone: {count} terms, {link_count} paragraph links{suffix}")


if __name__ == "__main__":
    main()
