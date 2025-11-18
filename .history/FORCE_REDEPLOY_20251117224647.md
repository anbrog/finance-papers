# Force Streamlit Cloud Redeploy

If you've updated requirements.txt but Streamlit Cloud still shows the old error, you need to force a redeploy:

## Option 1: Reboot the App (Recommended)
1. Go to your app: https://share.streamlit.io/
2. Click on your app `anbrog/finance-papers`
3. Click the **☰ menu** (hamburger icon) in the top right
4. Select **"Manage app"**
5. Click **"Reboot app"** button
6. Wait 1-2 minutes for the app to restart with fresh environment

## Option 2: Clear Cache & Redeploy
1. In the Manage app page
2. Click the **three dots menu** (⋮)
3. Select **"Clear cache"**
4. Then click **"Reboot app"**

## Option 3: Trigger New Commit (If Options 1-2 Don't Work)
Sometimes Streamlit Cloud's cache is stubborn. Force a new deployment:

```bash
# Make a trivial change to force redeploy
echo "" >> README.md
git add README.md
git commit -m "Force redeploy"
git push origin main
```

## Verify It Worked
After rebooting:
1. Go to your app URL
2. Click **"Update Data"** tab
3. Try **"Update Journal Articles"**
4. Should now work without the `requests` module error
