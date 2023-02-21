const { dialog } = require("electron").remote;

const { getCurrentWindow } = require("electron").remote;

const fs = require("fs");

const moment = require("moment");

const db = require("./helpers/Db");

const CsvParser = require("./helpers/CsvParser");
const CsvCreator = require("./helpers/CsvCreator");

const uploadBtn = document.getElementById("upload");
const consolidateFilesBtn = document.getElementById("consolidateFiles");
const beginDbImportBtn = document.getElementById("beginDbImport");
const runNatesQueryBtn = document.getElementById("runNatesQuery");
const resetBtn = document.getElementById("resetBtn");
const buildResultsBtn = document.getElementById("buildResults");

const mainLoader = document.querySelector(".main-loader");

const selectedFilesContainer = document.getElementById("selectedFilesToUpload");

var crdFile = __dirname + "/consolidated_residual_details.csv";
var crdPivotFile =
  __dirname + "/consolidated_residual_details_for_pivot_table.csv";

resetBtn.addEventListener("click", () => {
  showLoading();

  fs.exists(crdFile, async (exists) => {
    if (exists) {
      fs.unlink(crdFile, (err) => {
        if (err) {
          console.log(err);
        }
      });
    }

    fs.exists(crdPivotFile, async (exists) => {
      if (exists) {
        fs.unlink(crdPivotFile, (err) => {
          if (err) {
            console.log(err);
          }
        });
      }

      await db.reset();
      getCurrentWindow().reload();
    });
  });
});

uploadBtn.addEventListener("click", async () => {
  var resp = await dialog.showOpenDialog({
    properties: ["openFile", "multiSelections"],
  });
  for (let i = 0; i < resp.filePaths.length; i++) {
    const element = resp.filePaths[i];

    var li = document.createElement("li");
    li.classList.add("collection-item");
    li.innerText = element;
    selectedFilesContainer.appendChild(li);
  }
});

consolidateFilesBtn.addEventListener("click", async (e) => {
  try {
    showLoading();

    var files = selectedFilesContainer.querySelectorAll("li");

    if (files.length === 0) {
      M.toast({ html: "Files Required!" });
      hideLoading();
      return;
    }

    var recordsConsolidated = 0;

    for (let i = 0; i < files.length; i++) {
      const fileElement = files[i];
      const csv = await CsvParser.parseCsv(fileElement.innerText, {
        skipRows: 1,
      });
      const pattern = /\_(.*?)\_/;
      for (let x = 0; x < csv.data.length; x++) {
        var bnkid = pattern.exec(csv.fileName)[1];
        bnkid = bnkid.substring(4, bnkid.length);
        if (bnkid === "8200" || bnkid === "2900") {
          csv.data[x].bnkid = 2900;
        } else {
          csv.data[x].bnkid = Number.parseInt(bnkid);
        }
        csv.data[x].date = moment()
          .subtract(1, "months")
          .startOf("month")
          .format("MM/DD/YYYY");
      }

      recordsConsolidated += csv.data.length;

      if (csv) {
        CsvCreator.appendToCsv(crdFile, csv.data);
      }
    }

    var step1StatusIcon = document.getElementById("step1StatusIcon");
    step1StatusIcon.classList.remove("warning-icon");
    step1StatusIcon.innerText = "check";

    var step1StatusText = document.getElementById("step1StatusText");
    step1StatusText.innerText = `${recordsConsolidated} Processed`;

    var cardStepContentWrapper = document.getElementById(
      "step1CardContentWrapper"
    );
    cardStepContentWrapper.innerHTML = "";

    M.toast({ html: "CSV's consolidated!" });
  } catch (err) {
    console.log(err);
    M.toast({ html: err });
  }

  hideLoading();
});

beginDbImportBtn.addEventListener("click", async (e) => {
  try {
    showLoading();

    const csv = await CsvParser.parseCsv(crdFile);

    console.log(csv);

    var count = await db.importToDb(csv.data);

    var step2StatusIcon = document.getElementById("step2StatusIcon");
    step2StatusIcon.classList.remove("warning-icon");
    step2StatusIcon.innerText = "check";

    var step2StatusText = document.getElementById("step2StatusText");
    step2StatusText.innerText = `${count} Imported`;

    var cardStepContentWrapper = document.getElementById(
      "step2CardContentWrapper"
    );
    cardStepContentWrapper.innerHTML = "";

    M.toast({ html: "Records imported!" });
  } catch (err) {
    console.log(err);
    M.toast({ html: err });
  }

  hideLoading();
});

runNatesQueryBtn.addEventListener("click", async (e) => {
  try {
    showLoading();

    await db.runNatesQuery();

    var step3StatusIcon = document.getElementById("step3StatusIcon");
    step3StatusIcon.classList.remove("warning-icon");
    step3StatusIcon.innerText = "check";

    var step3StatusText = document.getElementById("step3StatusText");
    step3StatusText.innerText = `Completed`;

    var cardStepContentWrapper = document.getElementById(
      "step3CardContentWrapper"
    );
    cardStepContentWrapper.innerHTML = "";

    M.toast({ html: "Query ran successfully!" });
  } catch (err) {
    console.log(err);
    M.toast({ html: err });
  }

  hideLoading();
});

buildResultsBtn.addEventListener("click", async () => {
  try {
    showLoading();

    var results = await db.selectResultsFull();

    CsvCreator.appendToCsv(crdPivotFile, results);

    var resultSum = await db.selectResultsSum();
    console.log(
      `amtAfterCostExpenseSum ${resultSum.amtAfterCostExpenseSum.sum}`
    );
    console.log(
      `amtAfterCostRevenueSum ${resultSum.amtAfterCostRevenueSum.sum}`
    );
    console.log(`amtExpenseSum ${resultSum.amtExpenseSum.sum}`);
    console.log(`amtRevenueSum ${resultSum.amtRevenueSum.sum}`);

    var step4StatusIcon = document.getElementById("step4StatusIcon");
    step4StatusIcon.classList.remove("warning-icon");
    step4StatusIcon.innerText = "check";

    var step4StatusText = document.getElementById("step4StatusText");
    step4StatusText.innerText = `Completed`;

    var cardStepContentWrapper = document.getElementById(
      "step4CardContentWrapper"
    );
    cardStepContentWrapper.innerHTML = "";

    M.toast({ html: "Query ran successfully!" });
  } catch (err) {
    console.log(err);
    M.toast({ html: err });
  }

  hideLoading();
});

const showLoading = () => {
  mainLoader.classList.add("loading-shown");
};

const hideLoading = () => {
  mainLoader.classList.remove("loading-shown");
};
