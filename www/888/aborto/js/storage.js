// Gerador Whats Brasil | Módulo: storage.js
// Responsável por toda a interação com o localStorage.

const ANALYTICS_KEY = 'geradorWhatsAnalytics';
const HISTORY_KEY = 'geradorWhatsHistory';
const CUSTOM_TEMPLATES_KEY = 'geradorWhatsCustomTemplates';
const MAX_HISTORY_ITEMS = 10;

// Funções Genéricas
export function loadFromLocalStorage(key) {
    try {
        const data = localStorage.getItem(key);
        return data ? JSON.parse(data) : [];
    } catch (e) {
        console.error(`Erro ao ler do localStorage (key: ${key}):`, e);
        return [];
    }
}

export function saveToLocalStorage(key, data) {
    try {
        localStorage.setItem(key, JSON.stringify(data));
    } catch (e) {
        console.error(`Erro ao salvar no localStorage (key: ${key}):`, e);
    }
}

// Funções de Histórico
export function getHistory() {
    return loadFromLocalStorage(HISTORY_KEY);
}

export function saveToHistory(newItem) {
    let history = getHistory();
    history.unshift(newItem);
    if (history.length > MAX_HISTORY_ITEMS) {
        history.pop();
    }
    saveToLocalStorage(HISTORY_KEY, history);
}

export function deleteFromHistory(timestamp) {
    let history = getHistory();
    const newHistory = history.filter(h => h.timestamp !== timestamp);
    saveToLocalStorage(HISTORY_KEY, newHistory);
}

// Funções de Modelos Personalizados
export function getCustomTemplates() {
    return loadFromLocalStorage(CUSTOM_TEMPLATES_KEY);
}

export function saveCustomTemplate(newTemplate) {
    const templates = getCustomTemplates();
    templates.unshift(newTemplate);
    saveToLocalStorage(CUSTOM_TEMPLATES_KEY, templates);
}

export function deleteCustomTemplate(id) {
    const templates = getCustomTemplates();
    const newTemplates = templates.filter(t => t.id !== id);
    saveToLocalStorage(CUSTOM_TEMPLATES_KEY, newTemplates);
}

// Funções de Analytics
export function getTrackedLinks() {
    return loadFromLocalStorage(ANALYTICS_KEY);
}

export function saveTrackedLink(newLink) {
    const links = getTrackedLinks();
    links.unshift(newLink);
    saveToLocalStorage(ANALYTICS_KEY, links);
}

export function deleteTrackedLink(id) {
    const links = getTrackedLinks();
    const newLinks = links.filter(l => l.id !== id);
    saveToLocalStorage(ANALYTICS_KEY, newLinks);
}

export function incrementClickCount(linkId) {
    const links = getTrackedLinks();
    const linkIndex = links.findIndex(l => l.id === linkId);

    if (linkIndex > -1) {
        links[linkIndex].clicks++;
        saveToLocalStorage(ANALYTICS_KEY, links);
        return links[linkIndex].destinationUrl;
    }
    return null;
}

// Funções de Import/Export
export function getAllData() {
    return {
        history: getHistory(),
        customTemplates: getCustomTemplates(),
        analytics: getTrackedLinks()
    };
}

export function importData(data) {
    if (typeof data.history !== 'object' || typeof data.customTemplates !== 'object' || typeof data.analytics !== 'object') {
        throw new Error('Formato de dados do arquivo de backup inválido.');
    }
    saveToLocalStorage(HISTORY_KEY, data.history || []);
    saveToLocalStorage(CUSTOM_TEMPLATES_KEY, data.customTemplates || []);
    saveToLocalStorage(ANALYTICS_KEY, data.analytics || []);
}