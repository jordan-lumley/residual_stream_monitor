'use strict';

const { app, BrowserWindow, Menu } = require('electron')

const path = require('path'),
  fs = require('fs');

const createWindow = () => {
  const mainWindow = new BrowserWindow({
    width: 1000,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: true,
    }
  })

  mainWindow.loadFile('index.html')


  // var mainMenu = Menu.buildFromTemplate(mainMenuTemplate);

  // Menu.setApplicationMenu(mainMenu);

  resetFile();
}

const resetFile = () => {
  fs.exists(__dirname + "/consolidated_residual_details.csv", (exists) => {
    if (exists) {
      fs.unlink(__dirname + "/consolidated_residual_details.csv", async (err) => {
        if (err) {
          console.log(err);
        }
      });
    }
  });
  fs.exists(__dirname + "/consolidated_residual_details_for_pivot_table.csv", (exists) => {
    if (exists) {
      fs.unlink(__dirname + "/consolidated_residual_details_for_pivot_table.csv", async (err) => {
        if (err) {
          console.log(err);
        }
      });
    }
  });
};

// const mainMenuTemplate = [
//   {
//     label: "File",
//     submenu: [
//       {
//         label: "Quit",
//         click() {
//           app.quit();
//         }
//       }
//     ]
//   }
// ];

app.whenReady().then(createWindow)

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow()
})