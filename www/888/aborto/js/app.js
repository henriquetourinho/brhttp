// ======================================================================== //
// Gerador Whats Brasil | Módulo: app.js (v4.1 - Versão Final e Completa) //
// Responsável por orquestrar a aplicação e gerenciar eventos.              //
// ======================================================================== //

import * as storage from './storage.js';
import * as ui from './ui.js';
// O módulo vcard.js não é mais importado aqui, sua lógica foi integrada.

document.addEventListener('DOMContentLoaded', () => {

    // --- SELEÇÃO DE ELEMENTOS DO DOM ---
    const elements = {
        body: document.body,
        linkForm: document.getElementById('link-form'),
        numeroInput: document.getElementById('numero-whatsapp'),
        numeroVCardInput: document.getElementById('numero-vcard'),
        mensagemInput: document.getElementById('mensagem'),
        templateCategorySelect: document.getElementById('template-category'),
        templateListDiv: document.getElementById('template-list'),
        abrirWppBtn: document.getElementById('abrir-wpp-btn'),
        limparBtn: document.getElementById('limpar-btn'),
        copiarBtn: document.getElementById('copiar-btn'),
        resultArea: document.getElementById('result-area'),
        themeToggle: document.getElementById('theme-toggle'),
        qrDotColorInput: document.getElementById('qr-dot-color'),
        qrBgColorInput: document.getElementById('qr-bg-color'),
        qrLogoUpload: document.getElementById('qr-logo-upload'),
        qrDownloadBtn: document.getElementById('qr-download-btn'),
        qrDownloadFormat: document.getElementById('qr-download-format'),
        formatBoldBtn: document.getElementById('format-bold'),
        formatItalicBtn: document.getElementById('format-italic'),
        formatStrikeBtn: document.getElementById('format-strike'),
        customTemplateForm: document.getElementById('custom-template-form'),
        customTemplateTitleInput: document.getElementById('custom-template-title'),
        customTemplateTextInput: document.getElementById('custom-template-text'),
        generatorTypeRadios: document.querySelectorAll('input[name="generatorType"]'),
        vcardPhotoUpload: document.getElementById('vcard-photo-upload'),
        vcardPhotoPreview: document.getElementById('vcard-photo-preview'),
        trackClicksCheckbox: document.getElementById('track-clicks-checkbox'),
        linkNameGroup: document.getElementById('link-name-group'),
        linkNameInput: document.getElementById('link-name'),
        exportDataBtn: document.getElementById('export-data-btn'),
        importDataInput: document.getElementById('import-data-input'),
        variableInputsContainer: document.getElementById('variable-inputs-container'),
        variableInputTemplate: document.getElementById('variable-input-template'),
        vcardFieldsContainer: document.getElementById('vcard-fields-container'),
        // Elementos específicos do vCard para acesso direto
        vcardFirstName: document.getElementById('vcard-firstname'),
        vcardMiddleName: document.getElementById('vcard-middlename'),
        vcardLastName: document.getElementById('vcard-lastname'),
        vcardNickname: document.getElementById('vcard-nickname'),
        vcardEmail: document.getElementById('vcard-email'),
        vcardCompany: document.getElementById('vcard-company'),
        vcardTitle: document.getElementById('vcard-title'),
        vcardWebsite: document.getElementById('vcard-website'),
        vcardLinkedin: document.getElementById('vcard-linkedin'),
        vcardInstagram: document.getElementById('vcard-instagram'),
        vcardTwitter: document.getElementById('vcard-twitter'),
        vcardGithub: document.getElementById('vcard-github'),
        vcardTelegram: document.getElementById('vcard-telegram'),
        vcardYoutube: document.getElementById('vcard-youtube'),
        vcardReddit: document.getElementById('vcard-reddit'),
        vcardAddress: document.getElementById('vcard-address'),
        vcardCity: document.getElementById('vcard-city'),
        vcardNotes: document.getElementById('vcard-notes'),
        numeroVCardError: document.getElementById('numero-vcard-error')
    };

    // --- VARIÁVEIS DE ESTADO DA APLICAÇÃO ---
    let state = {
        itiWhatsapp: null,
        itiVCard: null,
        vcardPhotoBase64: '',
        currentGeneratorType: 'whatsapp',
        masterTemplateText: ''
    };
    
    // --- FUNÇÕES DE VALIDAÇÃO E GERAÇÃO DE VCARD (MOVIDAS DE VCARD.JS) ---

    function isValidEmail(email) {
        if (!email) return true; // Campo opcional
        const re = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
        return re.test(String(email).toLowerCase());
    }

    function isValidUrl(string) {
        if (!string) return true; // Campo opcional
        try {
            new URL(string);
            return true;
        } catch (_) {
            return false;
        }
    }

    function generateVCardString() {
        const {
            itiVCard, // Este é o itiVCard do 'state', não do 'elements'
            vcardFirstName, vcardMiddleName, vcardLastName, vcardNickname,
            vcardEmail, vcardCompany, vcardTitle,
            vcardWebsite, vcardLinkedin, vcardInstagram, vcardTwitter,
            vcardGithub, vcardTelegram, vcardYoutube, vcardReddit,
            vcardAddress, vcardCity, vcardNotes
        } = elements; // Usando elements para acessar os inputs diretamente

        // Validação Robusta
        if (!vcardFirstName.value || !vcardLastName.value) {
            alert('Por favor, preencha pelo menos o Primeiro Nome e o Apelido.');
            return null;
        }
        if (!state.itiVCard.isValidNumber()) { // Usando state.itiVCard
            if(elements.numeroVCardError) elements.numeroVCardError.textContent = 'O número de telefone parece inválido.';
            return null;
        }
        if (!isValidEmail(vcardEmail.value)) {
            alert('O endereço de e-mail inserido não parece ser válido.');
            return null;
        }
        if (!isValidUrl(vcardWebsite.value)) {
            alert('A URL do website inserida não parece ser válida. (Dica: inclua https://)');
            return null;
        }
        if (!isValidUrl(vcardLinkedin.value)) {
            alert('A URL do LinkedIn inserida não parece ser válida. (Dica: inclua https://)');
            return null;
        }
        if (!isValidUrl(vcardYoutube.value)) {
            alert('A URL do YouTube inserida não parece ser válida. (Dica: inclua https://)');
            return null;
        }

        const formattedName = `${vcardFirstName.value} ${vcardMiddleName.value} ${vcardLastName.value}`.replace(/\s+/g, ' ').trim();
        let vCardString = `BEGIN:VCARD\nVERSION:3.0\nN:${vcardLastName.value};${vcardFirstName.value};${vcardMiddleName.value};;\nFN:${formattedName}`;
        
        if (state.vcardPhotoBase64) vCardString += `\nPHOTO;ENCODING=b;TYPE=JPEG:${state.vcardPhotoBase64}`;
        if (vcardNickname.value) vCardString += `\nNICKNAME:${vcardNickname.value}`;
        vCardString += `\nTEL;TYPE=CELL:${state.itiVCard.getNumber()}`;
        if (vcardEmail.value) vCardString += `\nEMAIL:${vcardEmail.value.trim()}`;
        if (vcardCompany.value) vCardString += `\nORG:${vcardCompany.value.trim()}`;
        if (vcardTitle.value) vCardString += `\nTITLE:${vcardTitle.value.trim()}`;
        if (vcardWebsite.value) vCardString += `\nURL:${vcardWebsite.value.trim()}`;
        if (vcardLinkedin.value) vCardString += `\nURL;type=LinkedIn:${vcardLinkedin.value.trim()}`;
        if (vcardInstagram.value) vCardString += `\nX-SOCIALPROFILE;type=instagram:https://instagram.com/${vcardInstagram.value.replace('@', '').trim()}`;
        if (vcardTwitter.value) vCardString += `\nX-SOCIALPROFILE;type=twitter:https://twitter.com/${vcardTwitter.value.replace('@', '').trim()}`;
        if (vcardGithub.value) vCardString += `\nX-SOCIALPROFILE;type=github:https://github.com/${vcardGithub.value.trim()}`;
        if (vcardTelegram.value) vCardString += `\nX-SOCIALPROFILE;type=telegram:https://t.me/${vcardTelegram.value.replace('@', '').trim()}`;
        if (vcardYoutube.value) vCardString += `\nURL;type=YouTube:${vcardYoutube.value.trim()}`;
        if (vcardReddit.value) vCardString += `\nX-SOCIALPROFILE;type=reddit:https://www.reddit.com/user/${vcardReddit.value.replace('u/', '').trim()}`;
        if (vcardAddress.value || vcardCity.value) vCardString += `\nADR;TYPE=HOME:;;${vcardAddress.value.trim()};${vcardCity.value.trim()};;;`;
        if (vcardNotes.value) vCardString += `\nNOTE:${vcardNotes.value.replace(/\n/g, '\\n')}`;
        
        vCardString += `\nEND:VCARD`;

        return vCardString;
    }

    // --- FUNÇÕES DE LÓGICA PRINCIPAL ---
    
    function handleGeneration(e) {
        e.preventDefault();
        let qrCodeData = null;
        
        if (state.currentGeneratorType === 'whatsapp') {
            if (!state.itiWhatsapp.isValidNumber()) {
                ui.showError('numero-whatsapp-error', 'O número de telefone parece inválido.');
                return;
            }
            ui.showError('numero-whatsapp-error', '');
            
            const isTrackingChecked = elements.trackClicksCheckbox.checked;
            const destinationUrl = `https://wa.me/${state.itiWhatsapp.getNumber().replace(/\D/g, '')}?text=${encodeURIComponent(elements.mensagemInput.value)}`;

            if (isTrackingChecked) {
                const linkId = Math.random().toString(36).substr(2, 8);
                const trackableLink = `${window.location.origin}${window.location.pathname}#/track/${linkId}`;
                const linkName = elements.linkNameInput.value.trim() || destinationUrl;
                
                qrCodeData = trackableLink;
                ui.displayGeneratedLink(trackableLink, true);
                storage.saveTrackedLink({ id: linkId, name: linkName, destinationUrl, shortLink: trackableLink, clicks: 0, createdAt: new Date().toISOString() });
            } else {
                qrCodeData = destinationUrl;
                ui.displayGeneratedLink(destinationUrl, false);
                storage.saveToHistory({ number: state.itiWhatsapp.getNumber(), message: elements.mensagemInput.value, timestamp: Date.now() });
            }
        } else { // vCard
            // Chamando a função generateVCardString definida localmente no app.js
            qrCodeData = generateVCardString(); 
            if (!qrCodeData) return; // Se a validação falhar, generateVCardString retorna null
            ui.hideGeneratedLink();
        }

        if (qrCodeData) {
            ui.displayQRCode(qrCodeData);
        }
        loadAndRenderAllLists();
    }

    function processMessageForVariables() {
        state.masterTemplateText = elements.mensagemInput.value;
        const regex = /{{\s*([a-zA-Z0-9_]+)\s*}}/g;
        const matches = new Set();
        let match;
        while ((match = regex.exec(state.masterTemplateText)) !== null) {
            matches.add(match[1]);
        }
        
        const variables = Array.from(matches);
        elements.variableInputsContainer.innerHTML = '';

        if (variables.length === 0) {
            elements.variableInputsContainer.style.display = 'none';
        } else {
            variables.forEach(variableName => {
                const node = elements.variableInputTemplate.content.cloneNode(true);
                const label = node.querySelector('.variable-label');
                const input = node.querySelector('.variable-input');
                label.textContent = variableName.replace(/_/g, ' ');
                input.dataset.variable = variableName;
                elements.variableInputsContainer.appendChild(node);
            });
            elements.variableInputsContainer.style.display = 'flex';
        }
        updateMessageWithVariableValues();
    }

    function updateMessageWithVariableValues() {
        let newText = state.masterTemplateText;
        const inputs = elements.variableInputsContainer.querySelectorAll('.variable-input');
        inputs.forEach(input => {
            const variable = input.dataset.variable;
            const value = input.value;
            const placeholder = `{{${variable}}}`;
            const placeholderRegex = new RegExp(placeholder.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1"), 'g');
            newText = newText.replace(placeholderRegex, value || placeholder);
        });
        elements.mensagemInput.value = newText;
        updateAllPreviews();
    }

    function handleUIAction(action, data) {
        switch(action) {
            case 'reuse-history':
                elements.generatorTypeRadios[0].click();
                state.itiWhatsapp.setNumber(data.number);
                elements.mensagemInput.value = data.message;
                processMessageForVariables();
                window.scrollTo({ top: 0, behavior: 'smooth' });
                break;
            case 'delete-history':
                storage.deleteFromHistory(data);
                ui.renderHistory(handleUIAction);
                break;
            case 'use-template':
                 elements.generatorTypeRadios[0].click();
                 elements.mensagemInput.value = data.text;
                 processMessageForVariables();
                 window.scrollTo({ top: 0, behavior: 'smooth' });
                break;
            case 'delete-template':
                storage.deleteCustomTemplate(data);
                ui.renderCustomTemplates(handleUIAction);
                ui.renderTemplates(elements.templateCategorySelect.value);
                break;
            case 'delete-analytics':
                storage.deleteTrackedLink(data);
                ui.renderAnalytics(handleUIAction);
                break;
        }
    }

    function updateAllPreviews() {
        ui.updatePreview(state.currentGeneratorType, elements.vcardFieldsContainer, elements.mensagemInput);
    }
    
    function loadAndRenderAllLists() {
        ui.renderHistory(handleUIAction);
        ui.renderCustomTemplates(handleUIAction);
        ui.renderAnalytics(handleUIAction);
    }

    function handleFormat(char) {
        const start = elements.mensagemInput.selectionStart;
        const end = elements.mensagemInput.selectionEnd;
        if (end > start) {
            const text = elements.mensagemInput.value;
            elements.mensagemInput.value = `${text.substring(0, start)}${char}${text.substring(start, end)}${char}${text.substring(end)}`;
            updateAllPreviews();
            processMessageForVariables();
            elements.mensagemInput.focus();
            elements.mensagemInput.setSelectionRange(start + char.length, end + char.length);
        }
    }

    function attachEventListeners() {
        elements.linkForm.addEventListener('submit', handleGeneration);

        elements.generatorTypeRadios.forEach(radio => {
            radio.addEventListener('change', () => {
                state.currentGeneratorType = radio.value;
                ui.toggleGeneratorUI(state.currentGeneratorType);
                updateAllPreviews();
            });
        });

        elements.mensagemInput.addEventListener('input', processMessageForVariables);
        // Event listener para campos do vCard para atualizar a pré-visualização
        elements.vcardFieldsContainer.addEventListener('input', updateAllPreviews);
        elements.templateCategorySelect.addEventListener('change', e => ui.renderTemplates(e.target.value));

        elements.templateListDiv.addEventListener('click', e => {
            if (e.target.classList.contains('template-item')) {
                elements.mensagemInput.value = e.target.dataset.text;
                processMessageForVariables();
                elements.mensagemInput.focus();
            }
        });

        elements.variableInputsContainer.addEventListener('input', e => {
            if (e.target.classList.contains('variable-input')) {
                updateMessageWithVariableValues();
            }
        });

        elements.themeToggle.addEventListener('click', () => {
            const newTheme = elements.body.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
            elements.body.setAttribute('data-theme', newTheme);
            localStorage.setItem('theme', newTheme);
        });

        elements.limparBtn.addEventListener('click', () => {
            elements.linkForm.reset();
            elements.resultArea.style.display = 'none';
            ui.showError('numero-whatsapp-error', '');
            ui.showError('numero-vcard-error', '');
            state.itiWhatsapp.setNumber('');
            state.itiVCard.setNumber('');
            elements.vcardPhotoPreview.src = '';
            elements.vcardPhotoPreview.classList.add('hidden');
            state.vcardPhotoBase64 = '';
            elements.generatorTypeRadios[0].click();
            ui.toggleGeneratorUI('whatsapp');
            updateAllPreviews();
        });

        elements.abrirWppBtn.addEventListener('click', () => {
            if (state.itiWhatsapp.isValidNumber()) {
                window.open(`https://wa.me/${state.itiWhatsapp.getNumber().replace(/\D/g, '')}?text=${encodeURIComponent(elements.mensagemInput.value)}`, '_blank');
            } else {
                ui.showError('numero-whatsapp-error', 'Insira um número válido para abrir no WhatsApp.');
            }
        });

        elements.copiarBtn.addEventListener('click', (e) => {
            const link = document.getElementById('link-gerado').href;
            ui.copyToClipboard(link, e.currentTarget);
        });

        elements.customTemplateForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const title = elements.customTemplateTitleInput.value.trim();
            const text = elements.customTemplateTextInput.value.trim();
            if (title && text) {
                storage.saveCustomTemplate({ id: Date.now(), title, text });
                ui.renderCustomTemplates(handleUIAction);
                ui.renderTemplates(elements.templateCategorySelect.value);
                elements.customTemplateForm.reset();
            }
        });

        elements.qrDownloadBtn.addEventListener('click', () => ui.downloadQRCode(elements.qrDownloadFormat.value));
        elements.qrLogoUpload.addEventListener('change', e => {
            if (!e.target.files || !e.target.files[0]) return;
            const reader = new FileReader();
            reader.onload = (ev) => ui.updateQRCode({ image: ev.target.result });
            reader.readAsDataURL(e.target.files[0]);
        });
        elements.vcardPhotoUpload.addEventListener('change', e => {
            if (!e.target.files || !e.target.files[0]) return;
            const reader = new FileReader();
            reader.onload = (event) => {
                elements.vcardPhotoPreview.src = event.target.result;
                elements.vcardPhotoPreview.classList.remove('hidden');
                state.vcardPhotoBase64 = event.target.result.substring(event.target.result.indexOf(',') + 1);
            };
            reader.readAsDataURL(e.target.files[0]);
        });
        elements.qrDotColorInput.addEventListener('input', e => ui.updateQRCode({ dotsOptions: { color: e.target.value } }));
        elements.qrBgColorInput.addEventListener('input', e => ui.updateQRCode({ backgroundOptions: { color: e.target.value } }));
        
        elements.formatBoldBtn.addEventListener('click', () => handleFormat('*'));
        elements.formatItalicBtn.addEventListener('click', () => handleFormat('_'));
        elements.formatStrikeBtn.addEventListener('click', () => handleFormat('~'));
        
        elements.exportDataBtn.addEventListener('click', () => {
            const data = storage.getAllData();
            const jsonString = JSON.stringify(data, null, 2);
            const blob = new Blob([jsonString], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `gwbrasil_backup_${new Date().toISOString().slice(0, 10)}.json`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        });
        
        elements.importDataInput.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = (event) => {
                try {
                    const data = JSON.parse(event.target.result);
                    if (confirm('Atenção: Isto irá substituir todos os seus dados. Deseja continuar?')) {
                        storage.importData(data);
                        loadAndRenderAllLists();
                        ui.renderTemplates(elements.templateCategorySelect.value);
                        alert('Dados importados com sucesso!');
                    }
                } catch (err) { alert(`Erro ao ler o ficheiro: ${err.message}`); }
            };
            reader.readAsText(file);
        });

        elements.trackClicksCheckbox.addEventListener('change', e => {
            elements.linkNameGroup.classList.toggle('hidden', !e.target.checked);
        });
    }
    
    function checkForRedirection() {
        const hash = window.location.hash;
        if (hash.startsWith('#/track/')) {
            const linkId = hash.substring(8);
            const destinationUrl = storage.incrementClickCount(linkId);
            if (destinationUrl) {
                setTimeout(() => {
                    window.location.href = destinationUrl;
                }, 50);
            }
        }
    }

    function init() {
        checkForRedirection();
        
        const savedTheme = localStorage.getItem('theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
        elements.body.setAttribute('data-theme', savedTheme);
        
        const telInputOptions = { 
            utilsScript: "https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/19.2.16/js/utils.js", 
            initialCountry: "br", 
            separateDialCode: true 
        };
        
        // Adicionando try-catch para diagnosticar a inicialização do intlTelInput
        try {
            state.itiWhatsapp = window.intlTelInput(elements.numeroInput, telInputOptions);
            state.itiVCard = window.intlTelInput(elements.numeroVCardInput, telInputOptions);
        } catch (error) {
            console.error("Erro ao inicializar intlTelInput:", error);
            console.warn("Verifique se os arquivos CSS e JS da biblioteca intl-tel-input estão sendo carregados corretamente e se não há bloqueios de rede.");
        }

        loadAndRenderAllLists();
        ui.toggleGeneratorUI(state.currentGeneratorType);
        updateAllPreviews();
        attachEventListeners();
    }

    init();
});
