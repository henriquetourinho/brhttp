// ======================================================================== //
// Gerador Whats Brasil | Módulo: ui.js (v4.1 - Versão Final e Completa)  //
// Responsável por manipular o DOM e a interface do usuário.                //
// ======================================================================== //

import * as storage from './storage.js';

// --- VARIÁVEIS DO MÓDULO ---
let qrCodeInstance = null;
let copyTimeout = null;
let linkPreviewDebounce = null;

// --- DADOS (TEMPLATES PRÉ-DEFINIDOS) ---
const templates = {
    pessoal: [ { title: 'Convite de Aniversário', text: 'Olá, {{nome}}! 🎉 Gostaria de te convidar para a minha festa de aniversário no dia {{data}}, às {{hora}}, em {{local}}. A tua presença seria o melhor presente! Confirma se vens. Abraço!' }, { title: 'Lembrete de Compromisso', text: 'Oi, {{nome}}! A passar para lembrar do nosso café/reunião amanhã às {{hora}}. Até lá!' }, { title: 'Partilha de Localização', text: 'Olá! Já estou em {{local_atual}}. Segue a minha localização para nos encontrarmos. Avisa quando estiveres a chegar!' }, { title: 'Agradecimento por Presente', text: 'Olá, {{nome}}! Adorei o presente que me deste, muito obrigado(a) pelo carinho e por te lembrares! Significou muito para mim.' }, { title: 'Agradecimento por Ajuda', text: 'Queria agradecer imensamente pela tua ajuda com {{assunto}}. Salvaste o meu dia! Fico a dever-te uma.' }, { title: 'Organizar Viagem', text: 'Pessoal, a pensar em organizarmos aquela viagem para {{destino}} no fim de semana de {{data}}. Quem está dentro? Vamos combinar os detalhes.' }, { title: 'Pedido de Empréstimo', text: 'Olá, {{nome}}, tudo bem? Será que terias {{objeto}} para me emprestar por uns dias? Devolvo assim que terminar de usar. Obrigado!' }, { title: 'Parabéns por Conquista', text: 'Muitos parabéns pela tua nova conquista, {{conquista}}! Fiquei super feliz por ti. Mereces todo o sucesso! Vamos comemorar!' }, { title: 'Aviso de "A Caminho"', text: 'Estou a sair de casa agora, devo chegar em cerca de {{tempo_estimado}} minutos. Até já!' }, { title: 'Pedido de Opinião', text: 'Oi, {{nome}}! Estou a pensar em {{assunto}} e valorizo muito a tua opinião. O que achas sobre isto? Qualquer ideia é bem-vinda.' }, { title: 'Marcar Encontro', text: 'Olá! Que saudades. Estava a pensar em marcarmos um jantar para pôr a conversa em dia. Teria disponibilidade na próxima {{dia_da_semana}}?' }, { title: 'Notícia Urgente', text: 'Atenção, pessoal! Notícia importante sobre {{assunto}}. Por favor, leiam e respondam assim que puderem.' }, { title: 'Verificar Amigo', text: 'Oi, {{nome}}, há algum tempo que não falamos. Só a mandar uma mensagem para saber se está tudo bem contigo. Abraço!' }, { title: 'Pedido de Contacto', text: 'Olá! Perdi o contacto do(a) {{nome_da_pessoa}}. Por acaso não o(a) tens para me enviares? Obrigado!' }, { title: 'Convidar para Cinema/Série', text: 'E aí, {{nome}}! Vi que o filme/série {{nome_do_filme}} estreou. Que tal combinarmos para assistir juntos esta semana?' } ],
    empreendedor: [ { title: 'Networking Pós-Evento', text: 'Olá, {{nome}}! Foi um prazer conhecer-te no evento {{nome_do_evento}}. Adoraria conectar-me e, quem sabe, explorarmos sinergias futuras. O que achas?' }, { title: 'Envio de Orçamento', text: 'Prezado(a) {{nome_cliente}}, conforme conversámos, segue em anexo a nossa proposta para o serviço de {{servico}}. Fico à inteira disposição para esclarecer qualquer dúvida.' }, { title: 'Follow-up de Orçamento', text: 'Olá, {{nome_cliente}}, tudo bem? Gostaria de saber se tiveste a oportunidade de analisar a proposta que enviei. Há algo mais em que eu possa ajudar?' }, { title: 'Agendamento de Reunião', text: 'Olá, {{nome}}. Para discutirmos melhor o projeto {{nome_do_projeto}}, sugiro uma breve chamada. Teria disponibilidade na {{dia_da_semana}} às {{hora1}} ou {{hora2}}?' }, { title: 'Feedback Pós-Serviço', text: 'Olá, {{nome_cliente}}! Espero que estejas a gostar do nosso trabalho. O teu feedback é muito valioso. Poderias deixar um breve depoimento sobre a tua experiência?' }, { title: 'Anúncio de Promoção', text: 'Olá, cliente amigo! Temos uma promoção imperdível no nosso serviço/produto {{nome_do_produto}} apenas esta semana. Não percas! Gostaria de saber mais?' }, { title: 'Pedido de Indicação', text: 'Olá, {{nome_cliente}}! Fico feliz que tenhas gostado do nosso trabalho. Se conheceres alguém que também poderia beneficiar dos nossos serviços, ficaríamos muito gratos pela indicação.' }, { title: 'Prospeção Fria', text: 'Olá, {{nome}}! Vi que também fazes parte do grupo {{nome_do_grupo}} e reparei no teu excelente trabalho em {{area}}. Gostaria de apresentar como a minha solução de {{minha_solucao}} poderia ajudar-te.' }, { title: 'Aviso de Pagamento', text: 'Prezado(a) {{nome_cliente}}, este é um lembrete amigável sobre a fatura n.º {{numero_fatura}}, com vencimento em {{data_vencimento}}. Obrigado!' }, { title: 'Apresentação de Novo Serviço', text: 'Olá, {{nome_cliente}}! Como um cliente valioso, gostaria de apresentar em primeira mão o nosso novo serviço de {{novo_servico}}, que pode ser do teu interesse. Queres saber mais?' }, { title: 'Reativar Cliente Antigo', text: 'Olá, {{nome}}! Já há algum tempo que não conversamos. Lembrei-me de ti e gostaria de saber se há algo novo em que te possa ajudar. Temos novidades!' }, { title: 'Confirmação de Agendamento', text: 'Olá! A confirmar o nosso agendamento para o dia {{data}} às {{hora}}. Se precisares de reagendar, por favor, avisa com antecedência. Obrigado!' }, { title: 'Resposta Automática (Ausência)', text: 'Olá! Agradeço o teu contacto. De momento estou fora do meu horário de trabalho, mas responderei à tua mensagem assim que possível amanhã de manhã. Obrigado pela compreensão.' }, { title: 'Convite para Webinar/Workshop', text: 'Olá, {{nome}}! Convido-te a participar no nosso webinar gratuito sobre {{tema_do_webinar}} no dia {{data}}. Será uma ótima oportunidade para aprender mais. Inscreve-te aqui: {{link}}' }, { title: 'Agradecimento por Parceria', text: 'Olá, {{nome_parceiro}}! Gostaria de agradecer pela parceria de sucesso no projeto {{nome_do_projeto}}. Espero que possamos colaborar novamente em breve!' } ],
    empresas: [ { title: 'Boas-vindas a Novo Cliente', text: 'Prezado(a) {{nome_do_cliente}}, em nome de toda a equipa da {{nome_da_empresa}}, damos-lhe as boas-vindas! Estamos muito felizes por tê-lo(a) como nosso cliente.' }, { title: 'Suporte (Abertura de Ticket)', text: 'Olá! Recebemos a sua solicitação de suporte. O seu ticket é o n.º {{numero_ticket}}. Um dos nossos especialistas entrará em contacto em breve. Obrigado.' }, { title: 'Suporte (Resolução)', text: 'Informamos que a sua solicitação (Ticket {{numero_ticket}}) foi resolvida. Se precisar de mais alguma coisa, não hesite em contactar-nos. A {{nome_da_empresa}} agradece.' }, { title: 'Pesquisa de Satisfação (NPS)', text: 'Olá, {{nome_do_cliente}}. Numa escala de 0 a 10, qual a probabilidade de você recomendar a {{nome_da_empresa}} a um amigo ou colega? A sua opinião é fundamental para nós.' }, { title: 'Aviso de Manutenção', text: 'Aviso: No dia {{data}}, entre as {{hora_inicio}} e as {{hora_fim}}, os nossos serviços estarão em manutenção para melhorias. Pedimos desculpa por qualquer inconveniente.' }, { title: 'Confirmação de Encomenda', text: 'A sua encomenda n.º {{numero_encomenda}} foi confirmada e está a ser preparada para envio. Acompanharemos com mais detalhes em breve. Obrigado pela sua compra!' }, { title: 'Recuperação de Carrinho', text: 'Olá, {{nome_do_cliente}}! Reparámos que deixou alguns itens no seu carrinho na nossa loja. Gostaria de finalizar a sua compra? O seu carrinho está à sua espera.' }, { title: 'Comunicação de Crise', text: 'Atenção: Estamos cientes de um problema que afeta {{servico_afetado}}. A nossa equipa técnica já está a trabalhar na resolução com prioridade máxima. Iremos atualizando.' }, { title: 'Divulgação de Vaga', text: 'Estamos a contratar! A {{nome_da_empresa}} tem uma vaga aberta para {{cargo}}. Se conhece o candidato ideal, partilhe! Mais detalhes em: {{link_da_vaga}}' }, { title: 'Confirmação de Subscrição', text: 'Bem-vindo(a) à nossa newsletter! Confirmamos a sua subscrição. Prepare-se para receber as melhores dicas e novidades sobre {{tema}}.' }, { title: 'Aviso de Termos de Serviço', text: 'Informamos que os nossos Termos de Serviço e Política de Privacidade foram atualizados. Pode consultá-los em {{link_dos_termos}}. Obrigado por fazer parte da nossa comunidade.' }, { title: 'Convite para Programa Beta', text: 'Olá, cliente especial! Estamos a convidar um grupo selecionado para testar em primeira mão a nossa nova funcionalidade: {{nome_da_funcionalidade}}. Teria interesse em ser um testador beta?' }, { title: 'Gestão de Reclamações', text: 'Prezado(a) {{nome_do_cliente}}, recebemos a sua reclamação e lamentamos o sucedido. Estamos a analisar a sua situação internamente e daremos um retorno o mais breve possível.' }, { title: 'Lançamento de Produto', text: 'Chegou o grande dia! Temos o prazer de anunciar o lançamento do nosso novo {{produto_ou_servico}}. Descubra tudo em: {{link_do_produto}}' }, { title: 'Agradecimento de Fim de Ano', text: 'Nesta época festiva, toda a equipa da {{nome_da_empresa}} gostaria de agradecer pela sua confiança e parceria durante este ano. Desejamos-lhe umas Festas Felizes!' } ],
    criadores: [ { title: 'Proposta de Parceria', text: 'Olá, equipa da {{nome_da_marca}}! Sou criador(a) de conteúdo na área de {{nicho}} e um grande admirador do vosso trabalho. Gostaria de apresentar uma proposta de colaboração. O meu media kit está em anexo.' }, { title: 'Agradecimento Pós-Collab', text: 'Foi um prazer colaborar convosco na campanha {{nome_da_campanha}}. O feedback da minha audiência foi fantástico! Espero que possamos trabalhar juntos novamente no futuro.' }, { title: 'Divulgação de Novo Conteúdo', text: 'Olá, pessoal! Acabou de sair vídeo/artigo novo sobre {{tema_do_conteudo}}. Está imperdível! Confiram no link: {{link_do_conteudo}}' }, { title: 'Convite para Live', text: 'Alerta de Live! Na próxima {{dia_da_semana}} às {{hora}}, estarei ao vivo no meu {{plataforma}} para falar sobre {{tema_da_live}} com o convidado especial {{nome_do_convidado}}. Não percam!' }, { title: 'Resposta a Dúvidas (DM)', text: 'Olá! Muito obrigado pela tua mensagem. Essa é uma pergunta excelente e comum! Eu abordei esse tema em detalhe neste vídeo/post: {{link_do_conteudo}}. Espero que ajude!' }, { title: 'Venda de Infoproduto', text: 'Olá! Vi que te interessas por {{tema}}. Abri agora as inscrições para o meu curso/ebook {{nome_do_produto}}, que te vai ensinar a {{beneficio}}. As vagas são limitadas. Sabe mais aqui: {{link_de_venda}}' }, { title: 'Pedido de Feedback', text: 'Olá, {{nome_seguidor}}! A tua opinião é muito importante para mim. O que estás a achar dos últimos conteúdos? Há algum tema que gostarias que eu abordasse?' }, { title: 'Contacto para Imprensa', text: 'Olá, {{nome_jornalista}}. O meu nome é {{meu_nome}} e sou especialista em {{minha_area}}. Vi o seu recente artigo sobre {{tema}} e gostaria de me colocar à disposição para futuros comentários ou entrevistas.' }, { title: 'Cross-Promotion', text: 'E aí, {{nome_colega}}! Admiro muito o teu trabalho. Estava a pensar se não terias interesse em fazermos uma colaboração (uma live, um vídeo em conjunto) para as nossas audiências.' }, { title: 'Divulgação de Afiliação', text: 'Olá! Muitos de vocês perguntam sobre {{produto_que_uso}}. Eu uso e recomendo! Se decidirem comprar através do meu link de afiliado, estarão a apoiar o meu trabalho sem qualquer custo extra para vocês: {{link_afiliado}}' }, { title: 'Contacto a Patrocinadores', text: 'Assunto: Proposta de Parceria para {{nome_da_marca}}. Olá! O meu conteúdo sobre {{nicho}} atinge uma audiência de {{numero_seguidores}}, com um forte engajamento em {{dados_demograficos}}. Acredito que uma parceria seria mutuamente benéfica.' }, { title: 'Aviso de Férias', text: 'Olá, comunidade incrível! Estarei a fazer uma pequena pausa para recarregar energias entre {{data_inicio}} e {{data_fim}}. Voltarei em breve com mais e melhor conteúdo! Obrigado por tudo.' }, { title: 'Pedido de Depoimento (Alunos)', text: 'Olá, {{nome_aluno}}! Fico feliz por teres concluído o meu curso. Se tiveste uma boa experiência, poderias deixar um breve depoimento em vídeo ou texto? Ajudaria imensamente!' }, { title: 'Convite para Comunidade', text: 'Gostas do meu conteúdo? Então vais adorar a minha comunidade privada no {{plataforma}}! Lá partilho dicas exclusivas, bastidores e muito mais. Entra aqui: {{link_comunidade}}' }, { title: 'Lembrete de Evento', text: 'Estamos quase a começar! A nossa live/webinar sobre {{tema}} começa em 15 minutos. Garante o teu lugar e prepara as tuas perguntas! Link de acesso: {{link_do_evento}}' } ]
};

// --- FUNÇÕES INTERNAS DO MÓDULO ---

function parseAdvancedFormatting(text) {
    return text
        .replace(/\*(.*?)\*/g, '<b>$1</b>')
        .replace(/_(.*?)_/g, '<i>$1</i>')
        .replace(/~(.*?)~/g, '<s>$1</s>')
        .replace(/```(.*?)```/gs, '<code>$1</code>')
        .replace(/\n/g, '<br>')
        .replace(/^> (.*$)/gm, '<blockquote>$1</blockquote>');
}

async function fetchLinkMetadata(url) {
    // Usando um proxy CORS para evitar problemas de bloqueio
    const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`;
    try {
        const response = await fetch(proxyUrl);
        if (!response.ok) return null;
        const data = await response.json();
        const htmlContent = data.contents;
        if (!htmlContent) return null;
        
        const parser = new DOMParser();
        const doc = parser.parseFromString(htmlContent, 'text/html');
        
        const getMeta = (prop) => doc.querySelector(`meta[property='${prop}'], meta[name='${prop}']`)?.getAttribute('content') || '';
        
        return {
            title: getMeta('og:title') || doc.querySelector('title')?.textContent || 'Título não encontrado',
            description: getMeta('og:description') || getMeta('description') || '',
            image: getMeta('og:image') || '',
            siteName: getMeta('og:site_name') || url.split('/')[2]
        };
    } catch (error) {
        console.error("Erro ao buscar metadados do link:", error);
        return null;
    }
}

async function updateLinkPreviewUI(text) {
    const linkPreviewContainer = document.getElementById('link-preview-container');
    const linkPreviewTitle = document.getElementById('link-preview-title');
    const linkPreviewDescription = document.getElementById('link-preview-description');
    const linkPreviewUrl = document.getElementById('link-preview-url');
    const linkPreviewImage = document.getElementById('link-preview-image');

    const urlRegex = /(https?:\/\/[^\s]+)/;
    const match = text.match(urlRegex);

    if (!match || !linkPreviewContainer) {
        if(linkPreviewContainer) linkPreviewContainer.classList.add('hidden');
        return;
    }

    const url = match[0];
    linkPreviewContainer.classList.remove('hidden');
    linkPreviewTitle.textContent = 'A carregar pré-visualização...';
    linkPreviewDescription.textContent = '';
    linkPreviewImage.src = '';
    linkPreviewImage.style.display = 'none';

    const metadata = await fetchLinkMetadata(url);

    if (metadata) {
        linkPreviewTitle.textContent = metadata.title;
        linkPreviewDescription.textContent = metadata.description;
        linkPreviewUrl.textContent = metadata.siteName;
        if (metadata.image) {
            linkPreviewImage.src = metadata.image;
            linkPreviewImage.style.display = 'block';
        }
    } else {
        linkPreviewTitle.textContent = url;
        linkPreviewUrl.textContent = url.split('/')[2];
    }
}

function showEmptyMessage(container, type) {
    const messages = {
        history: 'Nenhum link foi gerado ainda.',
        templates: 'Você ainda não salvou nenhum modelo.',
        analytics: 'Você ainda não criou nenhum link rastreável.'
    };
    if (container) {
        container.innerHTML = `<p class="history-empty-message">${messages[type]}</p>`;
    }
}

// --- FUNÇÕES EXPORTADAS ---

export function copyToClipboard(text, element) {
    navigator.clipboard.writeText(text).then(() => {
        const originalContent = element.innerHTML;
        element.innerHTML = 'Copiado!';
        element.style.color = 'var(--cor-primaria)';
        clearTimeout(copyTimeout);
        copyTimeout = setTimeout(() => {
            element.innerHTML = originalContent;
            element.style.color = '';
        }, 2000);
    }).catch(err => {
        console.error('Erro ao copiar: ', err);
    });
}

export function updatePreview(generatorType, vcardFieldsContainer, mensagemInput) {
    const previewContactName = document.getElementById('preview-contact-name');
    const previewText = document.getElementById('preview-text');
    const previewTimestamp = document.getElementById('preview-timestamp');
    const linkPreviewContainer = document.getElementById('link-preview-container');

    if (generatorType === 'vcard') {
        const firstName = vcardFieldsContainer.querySelector('#vcard-firstname').value || '';
        const lastName = vcardFieldsContainer.querySelector('#vcard-lastname').value || '';
        previewContactName.textContent = `${firstName} ${lastName}`.trim() || 'Novo Contato';
        previewText.innerHTML = "As informações de contato para o QR Code aparecerão aqui quando gerado.";
        if(previewTimestamp) previewTimestamp.textContent = '';
        if(linkPreviewContainer) linkPreviewContainer.classList.add('hidden');
    } else {
        previewContactName.textContent = 'Novo Contato';
        const text = mensagemInput.value;
        previewText.innerHTML = parseAdvancedFormatting(text) || "Sua mensagem aparecerá aqui...";
        const now = new Date();
        previewTimestamp.textContent = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
        
        clearTimeout(linkPreviewDebounce);
        linkPreviewDebounce = setTimeout(() => {
            updateLinkPreviewUI(text);
        }, 500);
    }
}

export function renderTemplates(category) {
    const templateListDiv = document.getElementById('template-list');
    templateListDiv.innerHTML = '';
    const templatesToRender = category === 'custom' ? storage.getCustomTemplates() : templates[category];
    
    if (templatesToRender && templatesToRender.length > 0) {
        templatesToRender.forEach(template => {
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'template-item';
            button.textContent = template.title;
            button.dataset.text = template.text;
            templateListDiv.appendChild(button);
        });
    } else if (category === 'custom') {
        templateListDiv.innerHTML = '<p class="template-empty-message">Nenhum modelo personalizado salvo.</p>';
    }
}

export function toggleGeneratorUI(type) {
    const vcardFieldsContainer = document.getElementById('vcard-fields-container');
    const whatsappFieldsContainer = document.getElementById('whatsapp-fields-container');
    const gerarBtnText = document.getElementById('gerar-btn-text');
    const variableInputsContainer = document.getElementById('variable-inputs-container');
    const linkPreviewContainer = document.getElementById('link-preview-container');
    const isVCard = type === 'vcard';

    if (whatsappFieldsContainer) whatsappFieldsContainer.classList.toggle('hidden', isVCard);
    if (vcardFieldsContainer) vcardFieldsContainer.classList.toggle('hidden', !isVCard);

    if (gerarBtnText) {
        gerarBtnText.textContent = isVCard ? 'Gerar QR Code de Contato' : 'Gerar Link e QR Code';
    }

    if (!isVCard) {
        renderTemplates(document.getElementById('template-category').value);
    } else {
        if(variableInputsContainer) variableInputsContainer.style.display = 'none';
        if(linkPreviewContainer) linkPreviewContainer.classList.add('hidden');
    }
    
    document.querySelectorAll('.radio-label').forEach(label => {
        const input = label.querySelector('input[name="generatorType"]');
        if (input) {
            label.classList.toggle('active', input.value === type);
        }
    });
}

export function renderHistory(uiCallback) {
    const container = document.getElementById('history-list-container');
    const template = document.getElementById('history-item-template');
    container.innerHTML = '';
    const history = storage.getHistory();

    if (history.length === 0) {
        return showEmptyMessage(container, 'history');
    }

    history.forEach(item => {
        const clone = template.content.cloneNode(true);
        const link = `https://wa.me/${item.number.replace(/\D/g, '')}?text=${encodeURIComponent(item.message)}`;

        clone.querySelector('.history-item-number').textContent = item.number;
        clone.querySelector('.history-item-message').textContent = item.message || '(Sem mensagem)';
        
        // Adiciona e formata o horário de criação
        const createdAt = new Date(item.timestamp);
        const formattedTime = `${String(createdAt.getHours()).padStart(2, '0')}:${String(createdAt.getMinutes()).padStart(2, '0')}`;
        const formattedDate = `${String(createdAt.getDate()).padStart(2, '0')}/${String(createdAt.getMonth() + 1).padStart(2, '0')}/${createdAt.getFullYear()}`;
        
        // Assumindo que você tem um span ou div com a classe 'history-item-timestamp' no seu template HTML
        // Se não tiver, você precisará adicionar um no index.html dentro do <template id="history-item-template">
        const timestampElement = clone.querySelector('.history-item-timestamp');
        if (timestampElement) {
            timestampElement.textContent = `${formattedDate} ${formattedTime}`;
        } else {
            // Se não houver um elemento específico, adicione-o ao final da info
            const infoDiv = clone.querySelector('.history-item-info');
            if (infoDiv) {
                const newSpan = document.createElement('span');
                newSpan.className = 'history-item-timestamp';
                newSpan.textContent = `${formattedDate} ${formattedTime}`;
                infoDiv.appendChild(newSpan);
            }
        }

        clone.querySelector('.history-reuse-btn').addEventListener('click', () => uiCallback('reuse-history', item));
        clone.querySelector('.history-copy-btn').addEventListener('click', (e) => copyToClipboard(link, e.currentTarget));
        clone.querySelector('.history-delete-btn').addEventListener('click', () => uiCallback('delete-history', item.timestamp));

        container.appendChild(clone);
    });
}

export function renderCustomTemplates(uiCallback) {
    const container = document.getElementById('custom-template-list-container');
    const template = document.getElementById('custom-template-item-template');
    container.innerHTML = '';
    const customTemplates = storage.getCustomTemplates();

    if (customTemplates.length === 0) {
        return showEmptyMessage(container, 'templates');
    }

    customTemplates.forEach(item => {
        const clone = template.content.cloneNode(true);
        clone.querySelector('.custom-template-item-title').textContent = item.title;
        clone.querySelector('.custom-template-item-text').textContent = item.text;

        // Adiciona e formata o horário de criação para modelos personalizados
        const createdAt = new Date(item.id); // item.id é o timestamp para modelos personalizados
        const formattedTime = `${String(createdAt.getHours()).padStart(2, '0')}:${String(createdAt.getMinutes()).padStart(2, '0')}`;
        const formattedDate = `${String(createdAt.getDate()).padStart(2, '0')}/${String(createdAt.getMonth() + 1).padStart(2, '0')}/${createdAt.getFullYear()}`;
        
        const timestampElement = clone.querySelector('.custom-template-item-timestamp');
        if (timestampElement) {
            timestampElement.textContent = `${formattedDate} ${formattedTime}`;
        } else {
            const infoDiv = clone.querySelector('.custom-template-item-info');
            if (infoDiv) {
                const newSpan = document.createElement('span');
                newSpan.className = 'custom-template-item-timestamp';
                newSpan.textContent = `${formattedDate} ${formattedTime}`;
                infoDiv.appendChild(newSpan);
            }
        }

        clone.querySelector('.custom-template-use-btn').addEventListener('click', () => uiCallback('use-template', item));
        clone.querySelector('.custom-template-delete-btn').addEventListener('click', () => {
            if (confirm(`Tem certeza que deseja apagar o modelo "${item.title}"?`)) {
                uiCallback('delete-template', item.id);
            }
        });
        
        container.appendChild(clone);
    });
}

export function renderAnalytics(uiCallback) {
    const container = document.getElementById('analytics-list-container');
    const template = document.getElementById('analytics-item-template');
    container.innerHTML = '';
    const links = storage.getTrackedLinks();

    if (links.length === 0) {
        return showEmptyMessage(container, 'analytics');
    }

    links.forEach(link => {
        const clone = template.content.cloneNode(true);
        
        // --- Extrair e exibir a mensagem decodificada ---
        let decodedMessage = '';
        try {
            const urlObj = new URL(link.destinationUrl);
            const textParam = urlObj.searchParams.get('text');
            if (textParam) {
                decodedMessage = decodeURIComponent(textParam);
            }
        } catch (e) {
            console.error("Erro ao parsear URL de destino para analytics:", e);
            decodedMessage = link.destinationUrl; // Fallback para URL completa se houver erro
        }

        // O nome do link (se existir) ou a mensagem decodificada
        clone.querySelector('.analytics-item-name').textContent = link.name || decodedMessage;
        // A mensagem decodificada ou o URL completo de destino
        clone.querySelector('.analytics-item-destination').textContent = decodedMessage;
        
        // Link curto rastreável
        const shortlinkA = clone.querySelector('.analytics-item-shortlink');
        shortlinkA.href = link.shortLink;
        shortlinkA.textContent = link.shortLink.replace(/^https?:\/\//, '');

        // Visualização de cliques
        clone.querySelector('.click-count').textContent = link.clicks;

        // Adiciona e formata o horário de criação para links de analytics
        const createdAt = new Date(link.createdAt); // link.createdAt é a propriedade de timestamp
        const formattedTime = `${String(createdAt.getHours()).padStart(2, '0')}:${String(createdAt.getMinutes()).padStart(2, '0')}`;
        const formattedDate = `${String(createdAt.getDate()).padStart(2, '0')}/${String(createdAt.getMonth() + 1).padStart(2, '0')}/${createdAt.getFullYear()}`;
        
        const timestampElement = clone.querySelector('.analytics-item-timestamp');
        if (timestampElement) {
            timestampElement.textContent = `${formattedDate} ${formattedTime}`;
        } else {
            // Fallback se o elemento não for encontrado (adiciona ao info div)
            const infoDiv = clone.querySelector('.analytics-item-info-main'); // Alterado para o novo wrapper
            if (infoDiv) {
                const newSpan = document.createElement('span');
                newSpan.className = 'analytics-item-timestamp';
                newSpan.textContent = `${formattedDate} ${formattedTime}`;
                infoDiv.appendChild(newSpan);
            }
        }

        // Botões de ação
        clone.querySelector('.copy-analytics-link-btn').addEventListener('click', (e) => copyToClipboard(link.shortLink, e.currentTarget));
        clone.querySelector('.delete-analytics-link-btn').addEventListener('click', () => {
            if (confirm(`Tem certeza que deseja apagar o registro do link "${link.name}"?`)) {
                uiCallback('delete-analytics', link.id);
            }
        });

        container.appendChild(clone);
    });
}

export function displayGeneratedLink(link, isTrackable) {
    const resultArea = document.getElementById('result-area');
    const linkResultGroup = document.getElementById('link-result-group');
    const linkGeradoA = document.getElementById('link-gerado');

    if (linkResultGroup && linkGeradoA) {
        linkGeradoA.href = link;
        linkGeradoA.textContent = link.replace(/^https?:\/\//, '');
        linkResultGroup.classList.remove('hidden');
    }
    if(resultArea) resultArea.style.display = 'block';
}

export function hideGeneratedLink() {
    const resultArea = document.getElementById('result-area');
    const linkResultGroup = document.getElementById('link-result-group');
    if(linkResultGroup) linkResultGroup.classList.add('hidden');
    if(resultArea) resultArea.style.display = 'block';
}

export function displayQRCode(data) {
    const qrCodeContainer = document.getElementById('qrcode-container');
    const resultArea = document.getElementById('result-area');
    
    if (!qrCodeContainer || !resultArea) return;

    qrCodeContainer.innerHTML = '';
    const isDarkMode = document.body.getAttribute('data-theme') === 'dark';

    const qrDotColorInput = document.getElementById('qr-dot-color');
    const qrBgColorInput = document.getElementById('qr-bg-color');
    qrDotColorInput.value = isDarkMode ? "#EAEAEA" : "#111B21";
    qrBgColorInput.value = isDarkMode ? "#1F2937" : "#FFFFFF";
    
    // Acessando QRCodeStyling através do objeto global window
    // Adicionando uma verificação para garantir que QRCodeStyling está disponível
    // e um pequeno atraso para garantir que a biblioteca esteja totalmente carregada.
    if (typeof window.QRCodeStyling === 'undefined') {
        console.error("QRCodeStyling library is not loaded or not available in the window object. Retrying in 50ms...");
        // Tenta novamente após um pequeno atraso
        setTimeout(() => displayQRCode(data), 50); 
        return; 
    }

    qrCodeInstance = new window.QRCodeStyling({
        width: 200, // Ajustado de 250 para 200 para um tamanho mais compacto
        height: 200, // Ajustado de 250 para 200 para um tamanho mais compacto
        data: data,
        margin: 1, // Reduzido de 5 para 1 para um QR Code mais preenchido e denso
        dotsOptions: { color: qrDotColorInput.value, type: "rounded" },
        backgroundOptions: { color: qrBgColorInput.value },
        cornersSquareOptions: { type: "extra-rounded" },
        cornersDotOptions: { type: "dot" }
    });

    qrCodeInstance.append(qrCodeContainer);
    resultArea.style.display = 'block';
}

export function updateQRCode(options) {
    if (qrCodeInstance) {
        qrCodeInstance.update(options);
    }
}

export function downloadQRCode(format) {
    if (qrCodeInstance) {
        qrCodeInstance.download({ name: "gwbrasil-qrcode", extension: format });
    }
}

export function showError(elementId, message) {
    const errorSpan = document.getElementById(elementId);
    if (errorSpan) {
        errorSpan.textContent = message;
        errorSpan.style.display = message ? 'block' : 'none';
    }
}