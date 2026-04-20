// Gerador Whats Brasil | Módulo: app.js (Versão Corrigida)
// Responsável por orquestrar a aplicação e gerenciar eventos.

import * as storage from './storage.js';
import * as ui from './ui.js';
import * as vcard from './vcard.js';

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
        generatorTypeRadios: document.querySelectorAll('input[name="generatorType"]'),
        vcardPhotoUpload: document.getElementById('vcard-photo-upload'),
        vcardPhotoPreview: document.getElementById('vcard-photo-preview'),
        trackClicksCheckbox: document.getElementById('track-clicks-checkbox'),
        linkNameGroup: document.getElementById('link-name-group'),
        exportDataBtn: document.getElementById('export-data-btn'),
        importDataInput: document.getElementById('import-data-input'),
        variableInputsContainer: document.getElementById('variable-inputs-container')
    };

    // --- VARIÁVEIS DE ESTADO ---
    let state = {
        itiWhatsapp: null,
        itiVCard: null,
        vcardPhotoBase64: '',
        currentGeneratorType: 'whatsapp',
        masterTemplateText: ''
    };

    // --- FUNÇÕES DE LÓGICA PRINCIPAL ---
    
    function handleGeneration(e) {
        e.preventDefault();
        let qrCodeData;
        
        if (state.currentGeneratorType === 'whatsapp') {
            if (!state.itiWhatsapp.isValidNumber()) {
                document.getElementById('numero-whatsapp-error').textContent = 'O número de telefone parece inválido.';
                return;
            }
            document.getElementById('numero-whatsapp-error').textContent = '';
            
            const isTrackingChecked = elements.trackClicksCheckbox.checked;
            const destinationUrl = `https://wa.me/${state.itiWhatsapp.getNumber().replace(/\D/g, '')}?text=${encodeURIComponent(elements.mensagemInput.value)}`;

            if (isTrackingChecked) {
                const linkId = Math.random().toString(36).substr(2, 8);
                const trackableLink = `${window.location.origin}${window.location.pathname}#/track/${linkId}`;
                const linkName = document.getElementById('link-name').value.trim() || destinationUrl;
                
                qrCodeData = trackableLink;
                ui.displayGeneratedLink(trackableLink, true);
                storage.saveTrackedLink({ id: linkId, name: linkName, destinationUrl, shortLink: trackableLink, clicks: 0, createdAt: new Date().toISOString() });
            } else {
                qrCodeData = destinationUrl;
                ui.displayGeneratedLink(destinationUrl, false);
                storage.saveToHistory({ number: state.itiWhatsapp.getNumber(), message: elements.mensagemInput.value, timestamp: Date.now() });
            }
        } else { // vCard
            const vcardInputs = {
                itiVCard: state.itiVCard,
                vcardFirstName: document.getElementById('vcard-firstname').value,
                vcardMiddleName: document.getElementById('vcard-middlename').value,
                vcardLastName: document.getElementById('vcard-lastname').value,
                vcardNickname: document.getElementById('vcard-nickname').value,
                vcardPhotoBase64: state.vcardPhotoBase64,
                vcardEmail: document.getElementById('vcard-email').value,
                vcardCompany: document.getElementById('vcard-company').value,
                vcardTitle: document.getElementById('vcard-title').value,
                vcardWebsite: document.getElementById('vcard-website').value,
                vcardLinkedin: document.getElementById('vcard-linkedin').value,
                vcardInstagram: document.getElementById('vcard-instagram').value,
                vcardTwitter: document.getElementById('vcard-twitter').value,
                vcardGithub: document.getElementById('vcard-github').value,
                vcardTelegram: document.getElementById('vcard-telegram').value,
                vcardYoutube: document.getElementById('vcard-youtube').value,
                vcardReddit: document.getElementById('vcard-reddit').value,
                vcardAddress: document.getElementById('vcard-address').value,
                vcardCity: document.getElementById('vcard-city').value,
                vcardNotes: document.getElementById('vcard-notes').value
            };
            qrCodeData = vcard.generateVCardString(vcardInputs);
            if (!qrCodeData) return;
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
        const variableInputTemplate = document.getElementById('variable-input-template');
        elements.variableInputsContainer.innerHTML = '';

        if (variables.length === 0) {
            elements.variableInputsContainer.style.display = 'none';
        } else {
            variables.forEach(variableName => {
                const node = variableInputTemplate.content.cloneNode(true);
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
        const vcardInputs = {
            vcardFirstName: document.getElementById('vcard-firstname'),
            vcardLastName: document.getElementById('vcard-lastname')
        };
        ui.updatePreview(state.currentGeneratorType, vcardInputs, elements.mensagemInput);
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
        
        const vcardForm = document.getElementById('vcard-fields-container');
        vcardForm.addEventListener('input', updateAllPreviews);
        
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
            document.getElementById('numero-whatsapp-error').textContent = '';
            document.getElementById('numero-vcard-error').textContent = '';
            state.itiWhatsapp.setNumber('');
            state.itiVCard.setNumber('');
            elements.vcardPhotoPreview.classList.add('hidden');
            state.vcardPhotoBase64 = '';
            elements.generatorTypeRadios[0].click();
        });

        elements.abrirWppBtn.addEventListener('click', () => {
            if (state.itiWhatsapp.isValidNumber()) {
                window.open(`https://wa.me/${state.itiWhatsapp.getNumber().replace(/\D/g, '')}?text=${encodeURIComponent(elements.mensagemInput.value)}`, '_blank');
            } else {
                document.getElementById('numero-whatsapp-error').textContent = 'Insira um número válido para abrir no WhatsApp.';
            }
        });

        elements.copiarBtn.addEventListener('click', (e) => {
            const link = document.getElementById('link-gerado').href;
            ui.copyToClipboard(link, e.currentTarget);
        });

        elements.customTemplateForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const titleInput = document.getElementById('custom-template-title');
            const textInput = document.getElementById('custom-template-text');
            const title = titleInput.value.trim();
            const text = textInput.value.trim();
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
        
        state.itiWhatsapp = window.intlTelInput(elements.numeroInput, { utilsScript: "https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/19.2.16/js/utils.js", initialCountry: "br", separateDialCode: true });
        state.itiVCard = window.intlTelInput(elements.numeroVCardInput, { utilsScript: "https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/19.2.16/js/utils.js", initialCountry: "br", separateDialCode: true });

        loadAndRenderAllLists();
        const initialGeneratorType = 'whatsapp';
        state.currentGeneratorType = initialGeneratorType;
        ui.toggleGeneratorUI(initialGeneratorType);
        
        updateAllPreviews();
        attachEventListeners();
    }

    init();
});