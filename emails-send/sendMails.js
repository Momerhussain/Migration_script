// sendEmails.js
const fs = require('fs');
const path = require('path');
const nodemailer = require('nodemailer');
const { hideBin } = require('yargs/helpers');
const yargs = require('yargs/yargs');

// Set default values here
const defaultOptions = {
    to: "jobs@accelhr.com,recruit@bacme.com,info@charterhouse.ae,adeccoae.info@adecco.com,helpinghandsathr@outlook.com,nikki@nssearch.com,fm@melf.ae,finance@coralboutiquevilla.com,hr@alkhaleejpalacedeira.ae,emiratisation@michealpage.ae,hr@tafaseel.ae,recruitment@taleem.ae,talent@ergodubai.com,careers@infiniconcepts.com,info@ratlscontracting.com,judy@emiratalent.com,michelle@emiratalent.com,cess@emiratalent.com,maxime.lejuez@babylondifc.com,kenette.s@naischool.ae,hr@alimousa.ae,syed.naveed@itpeoplegulf.com,cv@primegroup.ae,hazem.sara@jac.ae,hr@vplaceugulfjobs.com,hr@avenuehoteldubai.com,luxury@hrsource.ae,career@baytik.ae,careers@terrasolisdubai.com,hr@prestigeluxury.ae,hr.leb@citruss.com,careers@vivandi.ae,mhnewbiz@gmail.com,ankitha@medichrc.com,career@abcrecruitment.ae,hireme@eyewa.com,hr@able.ae,cv@careerhubinternational.com,amara.ghafoor@gmail.com,recruit@dib.ae,careers@woodlemdubai.ae,careers@springfield-re.com,careers.mdab@milleniumhotels.com,ha9q6-cr@accor.com,recruitment@angloarabian-healthcare.com,info@deltaoilgasjobs.com,mc.secratary@habtoorhospitality.com,hr.metclub@habtoorhopitality.com,syeda.a@casamiauae.com,shahin@derbygroup.ae,abid.ali@meydanhotels.com,mahek@pactemloyment.ae,careers@inaya.ae,careers@arabiancementcompany.com,sara.mohamed@armadagroupco.com,british_grammarschool@yahoo.com,salah.khalil@movenpick.com,George@kingstonstanley.com,wenona@kinstonstanley.com,sumal.mohan@ardt.ae,hr@hialbarsha.com,vineyard@eim.ae,career@specenergy.com,hr@alghurair.com,maryaj@ajmal.net,vacantderma@gmail.com,careers@nabooda-auto.com,careers@alansari.ae,Resume@candidzone.net,hrad@gateshospitality.com,info@carino.ae,jobs@briteconsult.com,hr@erc.ae,careers@jannah-hotels.com,Hrd.exe14@psinv.net,hr@slhr-uae.com,recruitment@hskhospitality.com,Hr.ae@speedaf.com",
    subject: "Software Engineer",
    templatesDir: "./templates",
    attachmentsDir: "./attachments",
    fromName: "Omer Hussain"
  };


  // 3. List of possible subjects
const subjectOptions = [
    "Exploring Opportunities – Software Engineer",
    "Software Engineer – Open to Opportunities",
    "Interest in Software Engineering Opportunities",
  ];

  const argv = yargs(hideBin(process.argv))
  .option('to', {
    alias: 't', type: 'string',
    description: 'Comma-separated recipient emails',
    default: defaultOptions.to,
  })
  .option('subject', {
    alias: 's', type: 'string',
    description: 'Email subject',
    default: defaultOptions.subject,
  })
  .option('templatesDir', {
    alias: 'd', type: 'string',
    description: 'Directory containing .txt body templates',
    default: defaultOptions.templatesDir,
  })
  .option('attach', {
    alias: 'a', type: 'string',
    description: 'Path to one attachment file'
  })
  .option('attachmentsDir', {
    alias: 'D', type: 'string',
    description: 'Directory containing multiple attachment files',
    default: defaultOptions.attachmentsDir,
  })
  .option('fromName', {
    alias: 'n', type: 'string',
    description: 'Display name of sender',
    default: defaultOptions.fromName,
  })
  .check(argv => {
    if (!argv.attach && !argv.attachmentsDir)
      throw new Error('❌ Either --attach or --attachmentsDir must be provided');
    return true;
  })
  .help()
  .argv;

// Validate .env
// ['EMAIL_HOST', 'EMAIL_PORT', 'EMAIL_USER', 'EMAIL_PASS'].forEach(key => {
//   if (!process.env[key]) {
//     console.error(`❌ Missing ${key} in .env`);
//     process.exit(1);
//   }
// });

// Load templates
const templateFiles = fs.readdirSync(argv.templatesDir).filter(f => f.endsWith('.txt'));
if (!templateFiles.length) {
  console.error('❌ No .txt templates found in', argv.templatesDir);
  process.exit(1);
}
const templates = templateFiles.map(f => fs.readFileSync(path.join(argv.templatesDir, f), 'utf8'));

// Load attachments
let attachmentsList = [];
if (argv.attachmentsDir) {
  const files = fs.readdirSync(argv.attachmentsDir);
  if (!files.length) {
    console.error('❌ No files in attachmentsDir:', argv.attachmentsDir);
    process.exit(1);
  }
  attachmentsList = files.map(f => path.join(argv.attachmentsDir, f));
} else {
  attachmentsList = [path.resolve(argv.attach)];
}

// Setup transporter
const transporter = nodemailer.createTransport({
    service: "gmail",
  auth: {
    user:"momerhussain.eng@gmail.com",
    pass: "duqx warz vvaz jfvj",
  },
});

const recipients = argv.to.split(',').map(email => email.trim());
const pickRandom = arr => arr[Math.floor(Math.random() * arr.length)];

(async () => {
  let sentCount = 0;

  for (const to of recipients) {
    const body = pickRandom(templates);
    const attachmentPath = pickRandom(attachmentsList);

    try {
      const info = await transporter.sendMail({
        from: `"${argv.fromName}" <${'momerhussain.eng@gmail.com'}>`,
        to,
        subject: pickRandom(subjectOptions),
        text: body,
        attachments: [{
          filename: path.basename(attachmentPath),
          path: attachmentPath,
        }],
      });

      console.log(`✅ Sent to ${to} (template #${templates.indexOf(body) + 1}, file="${path.basename(attachmentPath)}")`);
      sentCount++;
    } catch (err) {
      console.error(`❌ Failed to send to ${to}: ${err.message}`);
    }
  }

  console.log(`\n🎉 Email process finished. ${sentCount}/${recipients.length} emails sent successfully.`);
})();
