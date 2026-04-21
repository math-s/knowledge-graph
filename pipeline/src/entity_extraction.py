"""Extract theological entities from CCC paragraphs using a controlled vocabulary."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from .models import Paragraph, resolve_lang

logger = logging.getLogger(__name__)


@dataclass
class EntityDefinition:
    """A theological entity with ID, label, category, and keyword patterns."""

    id: str
    label: str
    category: str
    keywords: list[str] = field(default_factory=list)


# ── Entity definitions ────────────────────────────────────────────────────────
# ~80+ theological terms organized by category.
# Keywords are matched with word boundaries and case-insensitivity.

ENTITY_DEFINITIONS: list[EntityDefinition] = [
    # ── Trinitarian ──────────────────────────────────────────────────────────
    EntityDefinition("trinity", "Trinity", "trinitarian",
        ["trinity", "triune", "triunity", "trindade", "triuno",
         "trinitas", "trinitatis", "trinitate"]),
    EntityDefinition("god-father", "God the Father", "trinitarian",
        ["god the father", "the father", "heavenly father",
         "deus pai", "pai celeste", "o pai",
         "deus pater", "pater caelestis", "pater omnipotens"]),
    EntityDefinition("god-son", "God the Son", "trinitarian",
        ["god the son", "the son of god", "son of man",
         "filho de deus", "filho do homem",
         "filius dei", "filius hominis"]),
    EntityDefinition("holy-spirit", "Holy Spirit", "trinitarian",
        ["holy spirit", "holy ghost", "paraclete", "spirit of god", "spirit of truth",
         "espírito santo", "paráclito", "espírito de deus",
         "spiritus sanctus", "paraclitus", "spiritus dei", "spiritus veritatis"]),
    EntityDefinition("consubstantial", "Consubstantial", "trinitarian",
        ["consubstantial", "homoousios", "consubstancial", "consubstantialis"]),
    EntityDefinition("filioque", "Filioque", "trinitarian", ["filioque"]),
    EntityDefinition("procession", "Divine Procession", "trinitarian",
        ["procession", "proceeds from", "procede de",
         "processio", "procedit a"]),

    # ── Christology ──────────────────────────────────────────────────────────
    EntityDefinition("incarnation", "Incarnation", "christology",
        ["incarnation", "incarnate", "word made flesh",
         "encarnação", "encarnado", "verbo se fez carne",
         "incarnatio", "incarnatus", "verbum caro factum"]),
    EntityDefinition("hypostatic-union", "Hypostatic Union", "christology",
        ["hypostatic union", "two natures",
         "união hipostática", "duas naturezas",
         "unio hypostatica", "duae naturae"]),
    EntityDefinition("paschal-mystery", "Paschal Mystery", "christology",
        ["paschal mystery", "mistério pascal", "mysterium paschale"]),
    EntityDefinition("redemption", "Redemption", "christology",
        ["redemption", "redeemer", "redeem",
         "redenção", "redentor", "remir",
         "redemptio", "redemptor"]),
    EntityDefinition("atonement", "Atonement", "christology",
        ["atonement", "expiation", "propitiation",
         "expiação", "propiciação",
         "expiatio", "propitiatio"]),
    EntityDefinition("resurrection", "Resurrection", "christology",
        ["resurrection", "risen from the dead", "empty tomb",
         "ressurreição", "ressuscitou dos mortos",
         "resurrectio", "resurrexit a mortuis"]),
    EntityDefinition("ascension", "Ascension", "christology",
        ["ascension", "ascended into heaven",
         "ascensão", "subiu ao céu",
         "ascensio", "ascendit in caelum"]),
    EntityDefinition("second-coming", "Second Coming", "christology",
        ["second coming", "parousia", "return of christ",
         "segunda vinda", "parusia",
         "adventus secundus", "parusia"]),
    EntityDefinition("messiah", "Messiah", "christology",
        ["messiah", "christ", "anointed",
         "messias", "cristo", "ungido",
         "christus", "messias", "unctus"]),
    EntityDefinition("kenosis", "Kenosis", "christology",
        ["kenosis", "self-emptying", "esvaziamento"]),

    # ── Sacraments ───────────────────────────────────────────────────────────
    EntityDefinition("baptism", "Baptism", "sacraments",
        ["baptism", "baptize", "baptized", "baptismal",
         "batismo", "batizar", "batizado", "batismal",
         "baptisma", "baptismus", "baptizare", "baptizatus"]),
    EntityDefinition("confirmation", "Confirmation", "sacraments",
        ["confirmation", "chrismation",
         "confirmação", "crisma",
         "confirmatio", "chrismatio"]),
    EntityDefinition("eucharist", "Eucharist", "sacraments",
        ["eucharist", "eucharistic", "holy communion",
         "eucaristia", "eucarística", "sagrada comunhão",
         "eucharistia", "eucharisticus", "sancta communio"]),
    EntityDefinition("transubstantiation", "Transubstantiation", "sacraments",
        ["transubstantiation", "real presence",
         "transubstanciação", "presença real",
         "transsubstantiatio", "praesentia realis"]),
    EntityDefinition("reconciliation", "Reconciliation", "sacraments",
        ["reconciliation", "confession", "penance",
         "reconciliação", "confissão", "penitência",
         "reconciliatio", "confessio", "paenitentia"]),
    EntityDefinition("anointing-sick", "Anointing of the Sick", "sacraments",
        ["anointing of the sick", "last rites", "viaticum",
         "unção dos enfermos", "extrema-unção", "viático",
         "unctio infirmorum", "extrema unctio", "viaticum"]),
    EntityDefinition("holy-orders", "Holy Orders", "sacraments",
        ["holy orders", "ordination", "ordained",
         "ordem sacra", "ordenação",
         "ordo sacer", "ordinatio"]),
    EntityDefinition("matrimony", "Matrimony", "sacraments",
        ["matrimony", "marriage", "nuptial",
         "matrimônio", "casamento", "nupcial",
         "matrimonium", "nuptiae"]),
    EntityDefinition("sacramental", "Sacramental Economy", "sacraments",
        ["sacramental economy", "sacramental",
         "economia sacramental", "sacramental"]),

    # ── Ecclesiology ─────────────────────────────────────────────────────────
    EntityDefinition("church", "Church", "ecclesiology",
        ["the church", "body of christ", "people of god",
         "a igreja", "corpo de cristo", "povo de deus",
         "ecclesia", "corpus christi", "populus dei"]),
    EntityDefinition("magisterium", "Magisterium", "ecclesiology",
        ["magisterium", "teaching authority",
         "magistério", "autoridade de ensino"]),
    EntityDefinition("apostolic-succession", "Apostolic Succession", "ecclesiology",
        ["apostolic succession", "sucessão apostólica", "successio apostolica"]),
    EntityDefinition("papacy", "Papacy", "ecclesiology",
        ["pope", "papacy", "roman pontiff", "vicar of christ", "holy see",
         "papa", "pontífice romano", "vigário de cristo", "santa sé",
         "pontifex romanus", "vicarius christi", "sancta sedes"]),
    EntityDefinition("episcopate", "Episcopate", "ecclesiology",
        ["bishop", "episcopate", "episcopal",
         "bispo", "episcopado",
         "episcopus", "episcopatus"]),
    EntityDefinition("collegiality", "Collegiality", "ecclesiology",
        ["collegiality", "college of bishops",
         "colegialidade", "colégio dos bispos",
         "collegium episcoporum"]),
    EntityDefinition("laity", "Laity", "ecclesiology",
        ["laity", "lay faithful", "laypeople",
         "laicato", "fiéis leigos", "leigos",
         "laici", "fideles laici"]),
    EntityDefinition("religious-life", "Religious Life", "ecclesiology",
        ["religious life", "consecrated life", "evangelical counsels",
         "vida religiosa", "vida consagrada", "conselhos evangélicos"]),
    EntityDefinition("ecumenism", "Ecumenism", "ecclesiology",
        ["ecumenism", "ecumenical", "christian unity",
         "ecumenismo", "ecumênico", "unidade cristã"]),
    EntityDefinition("communion-saints", "Communion of Saints", "ecclesiology",
        ["communion of saints", "comunhão dos santos", "communio sanctorum"]),

    # ── Soteriology ──────────────────────────────────────────────────────────
    EntityDefinition("salvation", "Salvation", "soteriology",
        ["salvation", "saved", "savior",
         "salvação", "salvo", "salvador",
         "salus", "salvatio", "salvator"]),
    EntityDefinition("justification", "Justification", "soteriology",
        ["justification", "justified",
         "justificação", "justificado",
         "justificatio", "justificatus"]),
    EntityDefinition("sanctification", "Sanctification", "soteriology",
        ["sanctification", "sanctify", "sanctified",
         "santificação", "santificar", "santificado",
         "sanctificatio", "sanctificare"]),
    EntityDefinition("grace", "Grace", "soteriology",
        ["grace", "sanctifying grace", "actual grace",
         "graça", "graça santificante", "graça atual",
         "gratia", "gratia sanctificans", "gratia actualis"]),
    EntityDefinition("merit", "Merit", "soteriology",
        ["merit", "mérito", "meritum"]),
    EntityDefinition("original-sin", "Original Sin", "soteriology",
        ["original sin", "fall of man",
         "pecado original", "queda do homem",
         "peccatum originale", "lapsus hominis"]),
    EntityDefinition("concupiscence", "Concupiscence", "soteriology",
        ["concupiscence", "concupiscência", "concupiscentia"]),
    EntityDefinition("predestination", "Predestination", "soteriology",
        ["predestination", "predestined",
         "predestinação", "predestinado",
         "praedestinatio", "praedestinatus"]),

    # ── Eschatology ──────────────────────────────────────────────────────────
    EntityDefinition("heaven", "Heaven", "eschatology",
        ["heaven", "beatific vision", "eternal life",
         "céu", "visão beatífica", "vida eterna",
         "caelum", "visio beatifica", "vita aeterna"]),
    EntityDefinition("hell", "Hell", "eschatology",
        ["hell", "eternal damnation", "gehenna",
         "inferno", "condenação eterna",
         "infernus", "gehenna", "damnatio aeterna"]),
    EntityDefinition("purgatory", "Purgatory", "eschatology",
        ["purgatory", "purification after death",
         "purgatório", "purificação após a morte",
         "purgatorium"]),
    EntityDefinition("particular-judgment", "Particular Judgment", "eschatology",
        ["particular judgment", "juízo particular", "iudicium particulare"]),
    EntityDefinition("last-judgment", "Last Judgment", "eschatology",
        ["last judgment", "final judgment", "general judgment",
         "juízo final", "juízo universal",
         "iudicium finale", "iudicium universale"]),
    EntityDefinition("resurrection-body", "Resurrection of the Body", "eschatology",
        ["resurrection of the body", "resurrection of the dead", "resurrection of the flesh",
         "ressurreição do corpo", "ressurreição da carne", "ressurreição dos mortos",
         "resurrectio carnis", "resurrectio mortuorum"]),
    EntityDefinition("new-creation", "New Creation", "eschatology",
        ["new heaven", "new earth", "new creation",
         "novo céu", "nova terra", "nova criação",
         "caelum novum", "terra nova", "nova creatio"]),

    # ── Mariology ────────────────────────────────────────────────────────────
    EntityDefinition("theotokos", "Theotokos", "mariology",
        ["theotokos", "mother of god", "mãe de deus", "mater dei"]),
    EntityDefinition("immaculate-conception", "Immaculate Conception", "mariology",
        ["immaculate conception", "imaculada conceição", "immaculata conceptio"]),
    EntityDefinition("assumption", "Assumption", "mariology",
        ["assumption of mary", "assumed into heaven",
         "assunção de maria", "assunta ao céu",
         "assumptio mariae", "assumpta in caelum"]),
    EntityDefinition("perpetual-virginity", "Perpetual Virginity", "mariology",
        ["perpetual virginity", "ever-virgin", "virgin mary",
         "virgindade perpétua", "sempre virgem", "virgem maria",
         "virginitas perpetua", "semper virgo", "virgo maria"]),
    EntityDefinition("marian-devotion", "Marian Devotion", "mariology",
        ["marian", "rosary", "magnificat",
         "mariano", "rosário", "magnificat"]),

    # ── Moral Theology ───────────────────────────────────────────────────────
    EntityDefinition("natural-law", "Natural Law", "moral", ["natural law", "natural moral law"]),
    EntityDefinition("conscience", "Conscience", "moral", ["conscience"]),
    EntityDefinition("moral-act", "Moral Act", "moral", ["moral act", "morality of human acts"]),
    EntityDefinition("common-good", "Common Good", "moral", ["common good"]),
    EntityDefinition("social-justice", "Social Justice", "moral", ["social justice", "social doctrine"]),
    EntityDefinition("human-dignity", "Human Dignity", "moral", ["human dignity", "dignity of the human person"]),
    EntityDefinition("solidarity", "Solidarity", "moral", ["solidarity"]),
    EntityDefinition("subsidiarity", "Subsidiarity", "moral", ["subsidiarity"]),
    EntityDefinition("decalogue", "Decalogue", "moral", ["decalogue", "ten commandments"]),
    EntityDefinition("beatitudes", "Beatitudes", "moral", ["beatitudes"]),

    # ── Virtues ──────────────────────────────────────────────────────────────
    EntityDefinition("faith", "Faith", "virtues", ["faith"]),
    EntityDefinition("hope", "Hope", "virtues", ["hope"]),
    EntityDefinition("charity", "Charity", "virtues", ["charity", "love of god", "love of neighbor"]),
    EntityDefinition("prudence", "Prudence", "virtues", ["prudence"]),
    EntityDefinition("justice", "Justice", "virtues", ["justice"]),
    EntityDefinition("fortitude", "Fortitude", "virtues", ["fortitude", "courage"]),
    EntityDefinition("temperance", "Temperance", "virtues", ["temperance"]),

    # ── Prayer & Liturgy ─────────────────────────────────────────────────────
    EntityDefinition("lords-prayer", "Lord's Prayer", "prayer", ["our father", "lord's prayer"]),
    EntityDefinition("contemplation", "Contemplation", "prayer", ["contemplation", "contemplative"]),
    EntityDefinition("meditation", "Meditation", "prayer", ["meditation"]),
    EntityDefinition("intercession", "Intercession", "prayer", ["intercession", "intercessory"]),
    EntityDefinition("liturgy", "Liturgy", "liturgy", ["liturgy", "liturgical"]),
    EntityDefinition("liturgical-year", "Liturgical Year", "liturgy", ["liturgical year", "advent", "lent", "easter"]),
    EntityDefinition("liturgy-hours", "Liturgy of the Hours", "liturgy", ["liturgy of the hours", "divine office"]),

    # ── Anthropology & Revelation ────────────────────────────────────────────
    EntityDefinition("imago-dei", "Imago Dei", "anthropology", ["image of god", "imago dei", "likeness of god"]),
    EntityDefinition("soul", "Soul", "anthropology", ["soul", "immortal soul"]),
    EntityDefinition("free-will", "Free Will", "anthropology", ["free will", "freedom"]),
    EntityDefinition("sacred-scripture", "Sacred Scripture", "revelation", ["sacred scripture", "scripture", "word of god"]),
    EntityDefinition("tradition", "Sacred Tradition", "revelation", ["sacred tradition", "apostolic tradition"]),
    EntityDefinition("revelation", "Divine Revelation", "revelation", ["divine revelation", "revelation"]),
    EntityDefinition("inspiration", "Biblical Inspiration", "revelation", ["inspiration", "inspired by god"]),
    EntityDefinition("deposit-faith", "Deposit of Faith", "revelation", ["deposit of faith"]),
    EntityDefinition("creation", "Creation", "anthropology", ["creation", "creator", "created the world"]),
    EntityDefinition("providence", "Divine Providence", "anthropology", ["divine providence", "providence"]),
]


# ── Pre-compiled regex patterns ──────────────────────────────────────────────

_ENTITY_PATTERNS: list[tuple[str, re.Pattern[str]]] = []


def _compile_patterns() -> None:
    """Pre-compile regex patterns for all entity keywords."""
    global _ENTITY_PATTERNS
    if _ENTITY_PATTERNS:
        return
    for entity in ENTITY_DEFINITIONS:
        for kw in entity.keywords:
            pattern = re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
            _ENTITY_PATTERNS.append((entity.id, pattern))


def extract_entities(text: str) -> list[str]:
    """Extract entity IDs from a text string.

    Returns a deduplicated list of entity IDs found in the text.
    """
    _compile_patterns()
    found: set[str] = set()
    for entity_id, pattern in _ENTITY_PATTERNS:
        if entity_id not in found and pattern.search(text):
            found.add(entity_id)
    return sorted(found)


def extract_all_entities(paragraphs: list[Paragraph]) -> list[Paragraph]:
    """Populate para.entities for all paragraphs.

    Uses English text for matching. Follows the assign_themes() pattern.
    """
    _compile_patterns()
    entity_counts: dict[str, int] = {}

    for para in paragraphs:
        text = resolve_lang(para.text, "en")
        entities = extract_entities(text)
        para.entities = entities

        for eid in entities:
            entity_counts[eid] = entity_counts.get(eid, 0) + 1

    # Log top entities
    for eid, count in sorted(entity_counts.items(), key=lambda x: -x[1])[:20]:
        logger.info("Entity '%s': %d paragraphs", eid, count)

    total_with_entities = sum(1 for p in paragraphs if p.entities)
    logger.info(
        "Extracted entities for %d/%d paragraphs (%d unique entities)",
        total_with_entities,
        len(paragraphs),
        len(entity_counts),
    )
    return paragraphs
