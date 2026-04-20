// Gerador Whats Brasil | Módulo: vcard.js
// Responsável pela validação e geração da string do vCard.

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

export function generateVCardString(inputs) {
    const {
        itiVCard,
        vcardFirstName, vcardMiddleName, vcardLastName, vcardNickname,
        vcardPhotoBase64, vcardEmail, vcardCompany, vcardTitle,
        vcardWebsite, vcardLinkedin, vcardInstagram, vcardTwitter,
        vcardGithub, vcardTelegram, vcardYoutube, vcardReddit,
        vcardAddress, vcardCity, vcardNotes
    } = inputs;

    // Validação Robusta
    if (!vcardFirstName || !vcardLastName) {
        alert('Por favor, preencha pelo menos o Primeiro Nome e o Apelido.');
        return null;
    }
    if (!itiVCard.isValidNumber()) {
        const errorSpan = document.getElementById('numero-vcard-error');
        if(errorSpan) errorSpan.textContent = 'O número de telefone parece inválido.';
        return null;
    }
     if (!isValidEmail(vcardEmail)) {
        alert('O endereço de e-mail inserido não parece ser válido.');
        return null;
    }
    if (!isValidUrl(vcardWebsite)) {
        alert('A URL do website inserida não parece ser válida. (Dica: inclua https://)');
        return null;
    }
     if (!isValidUrl(vcardLinkedin)) {
        alert('A URL do LinkedIn inserida não parece ser válida. (Dica: inclua https://)');
        return null;
    }
    if (!isValidUrl(vcardYoutube)) {
        alert('A URL do YouTube inserida não parece ser válida. (Dica: inclua https://)');
        return null;
    }

    const formattedName = `${vcardFirstName} ${vcardMiddleName} ${vcardLastName}`.replace(/\s+/g, ' ').trim();
    let vCardString = `BEGIN:VCARD\nVERSION:3.0\nN:${vcardLastName};${vcardFirstName};${vcardMiddleName};;\nFN:${formattedName}`;
    
    if (vcardPhotoBase64) vCardString += `\nPHOTO;ENCODING=b;TYPE=JPEG:${vcardPhotoBase64}`;
    if (vcardNickname) vCardString += `\nNICKNAME:${vcardNickname}`;
    vCardString += `\nTEL;TYPE=CELL:${itiVCard.getNumber()}`;
    if (vcardEmail) vCardString += `\nEMAIL:${vcardEmail.trim()}`;
    if (vcardCompany) vCardString += `\nORG:${vcardCompany.trim()}`;
    if (vcardTitle) vCardString += `\nTITLE:${vcardTitle.trim()}`;
    if (vcardWebsite) vCardString += `\nURL:${vcardWebsite.trim()}`;
    if (vcardLinkedin) vCardString += `\nURL;type=LinkedIn:${vcardLinkedin.trim()}`;
    if (vcardInstagram) vCardString += `\nX-SOCIALPROFILE;type=instagram:https://instagram.com/${vcardInstagram.replace('@', '').trim()}`;
    if (vcardTwitter) vCardString += `\nX-SOCIALPROFILE;type=twitter:https://twitter.com/${vcardTwitter.replace('@', '').trim()}`;
    if (vcardGithub) vCardString += `\nX-SOCIALPROFILE;type=github:https://github.com/${vcardGithub.trim()}`;
    if (vcardTelegram) vCardString += `\nX-SOCIALPROFILE;type=telegram:https://t.me/${vcardTelegram.replace('@', '').trim()}`;
    if (vcardYoutube) vCardString += `\nURL;type=YouTube:${vcardYoutube.trim()}`;
    if (vcardReddit) vCardString += `\nX-SOCIALPROFILE;type=reddit:https://www.reddit.com/user/${vcardReddit.replace('u/', '').trim()}`;
    if (vcardAddress || vcardCity) vCardString += `\nADR;TYPE=HOME:;;${vcardAddress.trim()};${vcardCity.trim()};;;`;
    if (vcardNotes) vCardString += `\nNOTE:${vcardNotes.replace(/\n/g, '\\n')}`;
    
    vCardString += `\nEND:VCARD`;

    return vCardString;
}